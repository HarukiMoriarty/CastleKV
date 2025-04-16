use serde::{Deserialize, Serialize};
use std::io;
use std::path::Path;
use tracing::{debug, info, warn};

/// Represents the persistent state required by the Raft consensus algorithm
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RaftPersistentState {
    /// Current term, increases monotonically
    pub current_term: u64,
    /// CandidateId that received vote in current term (or None)
    pub voted_for: Option<u32>,
}

impl Default for RaftPersistentState {
    fn default() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
        }
    }
}

/// Manages persistence of Raft state using sled
pub struct PersistentStateManager {
    /// Sled database instance
    db: sled::Db,
    /// Current state (cached)
    state: RaftPersistentState,
}

impl PersistentStateManager {
    /// Creates a new persistent state manager
    ///
    /// # Arguments
    ///
    /// * `base_path` - Base directory for storing state files
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let state_path = path.as_ref().to_path_buf();

        // Open sled database
        let db = sled::Config::new()
            .path(&state_path)
            .mode(sled::Mode::HighThroughput)
            .open()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // Try to load existing state or create default
        let state = match db.get("raft_state") {
            Ok(Some(data)) => match bincode::deserialize(&data) {
                Ok(state) => {
                    let state: RaftPersistentState = state;
                    info!(
                        "Loaded persistent state: term={}, voted_for={:?}",
                        state.current_term, state.voted_for
                    );
                    state
                }
                Err(e) => {
                    warn!("Failed to deserialize persistent state: {}", e);
                    RaftPersistentState::default()
                }
            },
            Ok(None) => {
                debug!("No existing state found, using defaults");
                RaftPersistentState::default()
            }
            Err(e) => {
                warn!("Failed to load persistent state: {}", e);
                RaftPersistentState::default()
            }
        };

        let manager = Self { db, state };

        // Persist initial state (in case default)
        manager.persist()?;

        Ok(manager)
    }

    /// Persists current state to sled
    fn persist(&self) -> io::Result<()> {
        let data =
            bincode::serialize(&self.state).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        self.db
            .insert("raft_state", data)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // Ensure data is flushed to disk
        self.db
            .flush()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        debug!(
            "Persisted state: term={}, voted_for={:?}",
            self.state.current_term, self.state.voted_for
        );

        Ok(())
    }

    /// Gets the current persistent state
    pub fn get_state(&self) -> RaftPersistentState {
        self.state
    }

    /// Updates the current term
    ///
    /// # Arguments
    ///
    /// * `term` - New term value
    pub fn update_term(&mut self, term: u64) -> io::Result<()> {
        if term > self.state.current_term {
            self.state.current_term = term;
            self.state.voted_for = None; // Reset vote when term changes
            self.persist()?;
        }
        Ok(())
    }

    /// Updates the voted_for field
    ///
    /// # Arguments
    ///
    /// * `candidate_id` - ID of the candidate that received the vote
    pub fn update_vote(&mut self, candidate_id: Option<u32>) -> io::Result<()> {
        self.state.voted_for = candidate_id;
        self.persist()
    }

    /// Updates both term and vote atomically
    ///
    /// # Arguments
    ///
    /// * `term` - New term value
    /// * `candidate_id` - ID of the candidate that received the vote
    pub fn update_term_and_vote(&mut self, term: u64, candidate_id: Option<u32>) -> io::Result<()> {
        if term >= self.state.current_term {
            self.state.current_term = term;
            self.state.voted_for = candidate_id;
            self.persist()?;
        }
        Ok(())
    }
}

impl Default for PersistentStateManager {
    fn default() -> Self {
        let db = sled::Config::new()
            .temporary(true)
            .open()
            .expect("Failed to open temporary sled database");

        Self {
            db,
            state: RaftPersistentState::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_persistence() -> io::Result<()> {
        let temp_dir = tempdir()?;

        // Create and initialize state
        let mut manager = PersistentStateManager::new(temp_dir.path())?;
        assert_eq!(manager.get_state().current_term, 0);
        assert_eq!(manager.get_state().voted_for, None);

        // Update state
        manager.update_term(5)?;
        manager.update_vote(Some(2))?;

        // Drop the manager to ensure it's flushed to disk
        drop(manager);

        // Create a new manager to verify persistence
        let manager2 = PersistentStateManager::new(temp_dir.path())?;
        assert_eq!(manager2.get_state().current_term, 5);
        assert_eq!(manager2.get_state().voted_for, Some(2));

        Ok(())
    }

    #[test]
    fn test_term_reset_vote() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let mut manager = PersistentStateManager::new(temp_dir.path())?;

        // Set initial state
        manager.update_term_and_vote(1, Some(2))?;
        assert_eq!(manager.get_state().current_term, 1);
        assert_eq!(manager.get_state().voted_for, Some(2));

        // Update to higher term should reset vote
        manager.update_term(2)?;
        assert_eq!(manager.get_state().current_term, 2);
        assert_eq!(manager.get_state().voted_for, None);

        Ok(())
    }

    #[test]
    fn test_default_manager() {
        let manager = PersistentStateManager::default();
        assert_eq!(manager.get_state().current_term, 0);
        assert_eq!(manager.get_state().voted_for, None);
    }
}
