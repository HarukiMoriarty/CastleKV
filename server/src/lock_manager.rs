use common::CommandId;
use std::collections::{HashMap, HashSet, VecDeque};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LockMode {
    Shared,
    Exclusive,
    Unlocked,
}

impl LockMode {
    fn conflict_with(&self, other: LockMode) -> bool {
        !matches!((self, other), (LockMode::Shared, LockMode::Shared))
    }
}

#[derive(Debug)]
pub enum AcquireLockResult {
    Acquired {
        other_owners: Vec<CommandId>,
        aborted_cmds: Vec<CommandId>,
    },
    Waiting {
        aborted_cmds: Vec<CommandId>,
    },
}

pub type LockManagerSender = mpsc::UnboundedSender<LockManagerMessage>;
type LockManagerReceiver = mpsc::UnboundedReceiver<LockManagerMessage>;

pub enum LockManagerMessage {
    AcquireLocks {
        cmd_id: CommandId,
        lock_requests: Vec<(String, LockMode)>,
        resp_tx: oneshot::Sender<bool>,
    },
    ReleaseLocks {
        cmd_id: CommandId,
    },
}

#[derive(Clone, Debug)]
struct Lock {
    mode: LockMode,
    owners: Vec<CommandId>,
    waiters: VecDeque<(CommandId, LockMode)>,
}

impl Lock {
    fn new(mode: LockMode, owner: CommandId) -> Self {
        Self {
            mode,
            owners: vec![owner],
            waiters: VecDeque::new(),
        }
    }

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
        // The current txn is not blocked by anyone
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

    fn release_lock(&mut self, cmd_id: CommandId) -> Vec<CommandId> {
        // Remove the current txn from the owners list, set the lock mode to unlocked if
        // no one else owns the lock
        self.owners.retain(|&id| id != cmd_id);
        if self.owners.is_empty() {
            self.mode = LockMode::Unlocked;
        }

        // Remove the current txn from the waiting list
        self.waiters.retain(|(id, _)| *id != cmd_id);

        // If no one is waiting, return immediately
        if self.waiters.is_empty() {
            self.mode = LockMode::Unlocked;
            return vec![];
        }

        grant_lock_to_waiters(&mut self.owners, &mut self.waiters, &mut self.mode)
    }
}

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

pub struct LockManager {
    rx: LockManagerReceiver,
    locks: HashMap<String, Lock>,
    cmd_channels: HashMap<CommandId, Vec<oneshot::Sender<bool>>>,
    // Track which locks are held by each command
    cmd_acquired_keys: HashMap<CommandId, HashSet<String>>,
    // Number of keys each waiting command still needs
    cmd_waiting_keys: HashMap<CommandId, usize>,
}

impl LockManager {
    pub fn new(rx: LockManagerReceiver) -> Self {
        Self {
            rx,
            locks: HashMap::new(),
            cmd_channels: HashMap::new(),
            cmd_acquired_keys: HashMap::new(),
            cmd_waiting_keys: HashMap::new(),
        }
    }

    fn handle_new_owners(&mut self, key: &str, new_owners: Vec<CommandId>) {
        for owner_id in new_owners {
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
                        let lock = self
                            .locks
                            .entry(key.clone())
                            .or_insert_with(|| Lock::new(*mode, cmd_id));

                        match lock.acquire_lock(cmd_id, *mode) {
                            AcquireLockResult::Acquired {
                                other_owners,
                                aborted_cmds,
                            } => {
                                acquired_keys.push(key.clone());
                                all_aborted_cmds.extend(aborted_cmds);
                                // Handle other owners that got granted this lock
                                self.handle_new_owners(key, other_owners);
                            }
                            AcquireLockResult::Waiting { aborted_cmds } => {
                                all_aborted_cmds.extend(aborted_cmds);
                                all_acquired = false;
                                break;
                            }
                        }
                    }

                    // Notify aborted commands
                    for cmd_to_abort in all_aborted_cmds {
                        if let Some(channels) = self.cmd_channels.remove(&cmd_to_abort) {
                            for resp in channels {
                                let _ = resp.send(false);
                            }
                        }
                    }

                    if all_acquired {
                        // Track acquired locks
                        let cmd_acquired_keys = self.cmd_acquired_keys.entry(cmd_id).or_default();
                        for key in acquired_keys {
                            cmd_acquired_keys.insert(key);
                        }
                        let _ = resp_tx.send(true);
                    } else {
                        // Store response channel for waiting
                        self.cmd_channels.entry(cmd_id).or_default().push(resp_tx);
                    }
                }

                LockManagerMessage::ReleaseLocks { cmd_id } => {
                    debug!("Releasing all locks for cmd {}", cmd_id);
                    if let Some(cmd_acquired_keys) = self.cmd_acquired_keys.remove(&cmd_id) {
                        for key in cmd_acquired_keys {
                            if let Some(lock) = self.locks.get_mut(&key) {
                                let new_owners = lock.release_lock(cmd_id);
                                self.handle_new_owners(&key, new_owners);
                            }
                        }
                    }
                    self.cmd_channels.remove(&cmd_id);
                }
            }
        }
    }
}
