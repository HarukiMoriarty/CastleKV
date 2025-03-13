use common::CommandId;
use std::collections::{HashMap, HashSet, VecDeque};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info};

/// Lock modes
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum LockMode {
    /// Multiple readers can hold the lock simultaneously
    Shared,
    /// Only a single writer can hold the lock
    Exclusive,
    /// No lock is held
    #[default]
    Unlocked,
}

impl LockMode {
    /// Check if this lock mode conflicts with another
    ///
    /// # Arguments
    ///
    /// * `other` - The other lock mode to check against
    fn conflict_with(&self, other: LockMode) -> bool {
        match self {
            // Shared locks conflict with exclusive locks
            LockMode::Shared => other == LockMode::Exclusive,
            // Exclusive locks conflict with any other lock
            LockMode::Exclusive => true,
            // Unlocked never conflicts
            LockMode::Unlocked => false,
        }
    }
}

/// Result of an attempt to acquire a lock
#[derive(Debug)]
pub enum AcquireLockResult {
    /// Lock was successfully acquired
    Acquired {
        /// Other commands that now also own the lock
        other_owners: Vec<CommandId>,
        /// Commands that were aborted due to conflicts
        aborted_cmds: Vec<CommandId>,
    },
    /// Command must wait for the lock
    Waiting {
        /// Commands that were aborted due to conflicts
        aborted_cmds: Vec<CommandId>,
    },
}

/// Channel sender type for the lock manager
pub type LockManagerSender = mpsc::UnboundedSender<LockManagerMessage>;
/// Channel receiver type for the lock manager
type LockManagerReceiver = mpsc::UnboundedReceiver<LockManagerMessage>;

/// Messages that can be sent to the lock manager
pub enum LockManagerMessage {
    /// Request to acquire locks
    AcquireLocks {
        /// Command ID requesting the locks
        cmd_id: CommandId,
        /// List of keys and lock modes to acquire
        lock_requests: Vec<(String, LockMode)>,
        /// Channel for sending the response
        resp_tx: oneshot::Sender<bool>,
    },
    /// Request to release all locks held by a command
    ReleaseLocks {
        /// Command ID releasing locks
        cmd_id: CommandId,
    },
}

/// Represents a lock on a single key
#[derive(Clone, Debug, Default)]
struct Lock {
    /// Current mode of the lock
    mode: LockMode,
    /// Commands that currently own the lock
    owners: Vec<CommandId>,
    /// Commands waiting to acquire the lock
    waiters: VecDeque<(CommandId, LockMode)>,
}

impl Lock {
    /// Attempt to acquire the lock
    ///
    /// # Arguments
    ///
    /// * `cmd_id` - Command ID requesting the lock
    /// * `lock_mode` - Mode of the lock being requested
    ///
    /// # Returns
    ///
    /// * `AcquireLockResult` - Result of the acquisition attempt
    fn acquire_lock(&mut self, cmd_id: CommandId, lock_mode: LockMode) -> AcquireLockResult {
        let mut aborted_cmds = Vec::new();

        // A group refers to consecutive waiters with the same lock mode
        let mut num_group_members = 0;
        let mut num_aborted_group_members = 0;
        let mut prev_lock_mode = LockMode::Unlocked;

        // Iterate from the back of the waiters list
        for (waiter_id, waiter_lock_mode) in self.waiters.iter_mut().rev() {
            if *waiter_lock_mode != prev_lock_mode {
                // End previous group. If the previous group conflicts with us and
                // not all members of the group are aborted, we can stop iterating.
                if prev_lock_mode.conflict_with(lock_mode)
                    && num_aborted_group_members < num_group_members
                {
                    break;
                }
                // Start new group
                num_group_members = 0;
                num_aborted_group_members = 0;
            }

            // Update the number of members in the group
            num_group_members += 1;

            // Abort conflicting commands with higher IDs (younger)
            if waiter_lock_mode.conflict_with(lock_mode) && *waiter_id > cmd_id {
                // Save the aborted cmds
                aborted_cmds.push(*waiter_id);
                // Set the waiter to invalid so that we can remove it later
                *waiter_id = CommandId::INVALID;
                // Update the number of aborted members in the group
                num_aborted_group_members += 1;
            }

            prev_lock_mode = *waiter_lock_mode;
        }

        // Remove aborted waiters
        self.waiters
            .retain(|(waiter_id, _)| *waiter_id != CommandId::INVALID);

        // If blocked by last waiter group
        if prev_lock_mode.conflict_with(lock_mode) && num_aborted_group_members < num_group_members
        {
            // The current cmd has to wait
            self.waiters.push_back((cmd_id, lock_mode));
            AcquireLockResult::Waiting { aborted_cmds }
        }
        // The current cmd is not blocked in the waiters list but is blocked by the current owners
        else if self.mode.conflict_with(lock_mode) {
            for owner_id in &self.owners {
                if *owner_id > cmd_id {
                    aborted_cmds.push(*owner_id);
                }
            }

            if insert_to_owners(&mut self.owners, &mut self.mode, cmd_id, lock_mode) {
                AcquireLockResult::Acquired {
                    other_owners: vec![],
                    aborted_cmds,
                }
            } else {
                self.waiters.push_back((cmd_id, lock_mode));
                AcquireLockResult::Waiting { aborted_cmds }
            }
        }
        // The current cmd is not blocked by anyone
        else {
            // This must succeed
            assert!(insert_to_owners(
                &mut self.owners,
                &mut self.mode,
                cmd_id,
                lock_mode
            ));
            let other_owners =
                grant_lock_to_waiters(&mut self.owners, &mut self.waiters, &mut self.mode);
            AcquireLockResult::Acquired {
                other_owners,
                aborted_cmds,
            }
        }
    }

    /// Release a lock held by a command
    ///
    /// # Arguments
    ///
    /// * `cmd_id` - Command ID releasing the lock
    ///
    /// # Returns
    ///
    /// * `Vec<CommandId>` - Commands that now own the lock after release
    fn release_lock(&mut self, cmd_id: CommandId) -> Vec<CommandId> {
        // Remove the current cmd from the owners list, set the lock mode to unlocked if
        // no one else owns the lock
        self.owners.retain(|&id| id != cmd_id);
        if self.owners.is_empty() {
            self.mode = LockMode::Unlocked;
        }

        // Remove the current cmd from the waiting list
        self.waiters.retain(|(id, _)| *id != cmd_id);

        // If no one is waiting, return immediately
        if self.waiters.is_empty() {
            self.mode = LockMode::Unlocked;
            return vec![];
        }

        grant_lock_to_waiters(&mut self.owners, &mut self.waiters, &mut self.mode)
    }
}

/// Insert a command into the owners list
///
/// # Arguments
///
/// * `owners` - List of command IDs that own the lock
/// * `owners_mode` - Current mode of the lock
/// * `cmd_id` - Command ID to insert
/// * `mode` - Mode of the current command
///
/// # Returns
///
/// * `bool` - True if insertion succeeded, false otherwise
fn insert_to_owners(
    owners: &mut Vec<CommandId>,
    owners_mode: &mut LockMode,
    cmd_id: CommandId,
    mode: LockMode,
) -> bool {
    if owners_mode.conflict_with(mode) {
        // If the waiting cmd is already the only owner of this lock
        // and lock mode is Shared the upgrade to Exclusive
        let is_already_the_only_owner = owners.len() == 1 && owners[0] == cmd_id;
        if is_already_the_only_owner && *owners_mode == LockMode::Shared {
            *owners_mode = mode;
        }
        is_already_the_only_owner
    } else {
        owners.push(cmd_id);
        *owners_mode = mode;
        true
    }
}

/// Grant locks to waiting commands
///
/// # Arguments
///
/// * `owners` - List of command IDs that own the lock
/// * `waiters` - Queue of commands waiting for the lock
/// * `mode` - Current mode of the lock
///
/// # Returns
///
/// * `Vec<CommandId>` - Commands that were granted the lock
fn grant_lock_to_waiters(
    owners: &mut Vec<CommandId>,
    waiters: &mut VecDeque<(CommandId, LockMode)>,
    mode: &mut LockMode,
) -> Vec<CommandId> {
    let mut new_owners = Vec::new();
    while let Some((waiter, waiter_mode)) = waiters.front() {
        let cmd_id = *waiter;
        if !insert_to_owners(owners, mode, cmd_id, *waiter_mode) {
            break;
        }
        new_owners.push(cmd_id);
        waiters.pop_front();
    }
    new_owners
}

/// Lock manager for handling distributed locks
pub struct LockManager {
    /// Channel for receiving lock manager messages
    rx: LockManagerReceiver,
    /// Map of key to lock
    locks: HashMap<String, Lock>,
    /// Map of command ID to response channels
    cmd_channels: HashMap<CommandId, Vec<oneshot::Sender<bool>>>,
    /// Map of command ID to set of acquired keys
    cmd_acquired_keys: HashMap<CommandId, HashSet<String>>,
    /// Map of command ID to count of keys it's still waiting for
    cmd_waiting_keys: HashMap<CommandId, usize>,
}

impl LockManager {
    /// Create a new lock manager
    pub fn new(rx: LockManagerReceiver) -> Self {
        Self {
            rx,
            locks: HashMap::new(),
            cmd_channels: HashMap::new(),
            cmd_acquired_keys: HashMap::new(),
            cmd_waiting_keys: HashMap::new(),
        }
    }

    /// Handle commands that have gained ownership of locks
    ///
    /// # Arguments
    ///
    /// * `key` - The key that has new owners
    /// * `new_owners` - The command IDs that now own the lock
    fn handle_new_owners(&mut self, key: &str, new_owners: Vec<CommandId>) {
        for owner_id in new_owners {
            debug!("{owner_id:?} gained lock on {key:?}");

            // Add this key to their acquired locks
            self.cmd_acquired_keys
                .entry(owner_id)
                .or_default()
                .insert(key.to_string());

            // Decrease their remaining keys count
            if let Some(count) = self.cmd_waiting_keys.get_mut(&owner_id) {
                *count -= 1;

                // If they've gotten all their keys, notify them
                if *count == 0 {
                    debug!("Notifying {owner_id:?} - all locks acquired");
                    if let Some(channels) = self.cmd_channels.remove(&owner_id) {
                        for resp in channels {
                            let _ = resp.send(true);
                        }
                    }
                    self.cmd_waiting_keys.remove(&owner_id);
                }
            }
        }
    }

    /// Run the lock manager
    pub async fn run(mut self) {
        info!("Lock manager started");

        while let Some(msg) = self.rx.recv().await {
            match msg {
                LockManagerMessage::AcquireLocks {
                    cmd_id,
                    lock_requests,
                    resp_tx,
                } => {
                    debug!(
                        "Lock request: command {:?} requesting {:?}",
                        cmd_id, lock_requests
                    );

                    let mut all_acquired = true;
                    let mut all_aborted_cmds = HashSet::new();
                    let mut acquired_keys = Vec::new();

                    // Try to acquire all locks
                    for (key, mode) in &lock_requests {
                        let lock = self.locks.entry(key.clone()).or_default();
                        debug!("Key {key:?} - Current lock state: {lock:?}");

                        match lock.acquire_lock(cmd_id, *mode) {
                            AcquireLockResult::Acquired {
                                other_owners,
                                aborted_cmds,
                            } => {
                                debug!("Acquired lock on {key:?}: other owners {other_owners:?}, aborted cmds {aborted_cmds:?}");
                                acquired_keys.push(key.clone());
                                all_aborted_cmds.extend(aborted_cmds);

                                // Handle other owners that got granted this lock
                                self.handle_new_owners(key, other_owners);
                            }
                            AcquireLockResult::Waiting { aborted_cmds } => {
                                debug!(
                                    "Waiting for lock on {key:?}: aborted cmds {aborted_cmds:?}"
                                );
                                all_aborted_cmds.extend(aborted_cmds);
                                all_acquired = false;
                                break;
                            }
                        }
                    }

                    // Notify aborted commands
                    for cmd_to_abort in all_aborted_cmds {
                        debug!("Notifying aborted command {cmd_to_abort:?}");
                        if let Some(channels) = self.cmd_channels.remove(&cmd_to_abort) {
                            for resp in channels {
                                let _ = resp.send(false);
                            }
                        }
                    }

                    // Handle result for the requesting command
                    if all_acquired {
                        // Track acquired locks
                        let cmd_acquired_keys = self.cmd_acquired_keys.entry(cmd_id).or_default();
                        for key in acquired_keys {
                            cmd_acquired_keys.insert(key);
                        }

                        debug!("Command {cmd_id:?} acquired all requested locks");
                        let _ = resp_tx.send(true);
                    } else {
                        // Store response channel for waiting
                        debug!(
                            "Command {cmd_id:?} is waiting for {} locks",
                            lock_requests.len() - acquired_keys.len()
                        );
                        self.cmd_channels.entry(cmd_id).or_default().push(resp_tx);
                        *self.cmd_waiting_keys.entry(cmd_id).or_default() +=
                            lock_requests.len() - acquired_keys.len();
                    }
                }

                LockManagerMessage::ReleaseLocks { cmd_id } => {
                    debug!("Releasing all locks for command {cmd_id:?}");

                    if let Some(cmd_acquired_keys) = self.cmd_acquired_keys.remove(&cmd_id) {
                        for key in cmd_acquired_keys {
                            if let Some(lock) = self.locks.get_mut(&key) {
                                debug!("Releasing lock on {key:?}");
                                let new_owners = lock.release_lock(cmd_id);
                                self.handle_new_owners(&key, new_owners);
                            }
                        }
                    }

                    // Clean up any pending channels or waiting keys
                    self.cmd_channels.remove(&cmd_id);
                    self.cmd_waiting_keys.remove(&cmd_id);
                }
            }
        }

        info!("Lock manager stopped");
    }
}
