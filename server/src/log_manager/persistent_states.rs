use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

/// Represents the persistent state required by the Raft consensus algorithm
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct RaftPersistentState {
    /// Current term, increases monotonically
    pub current_term: u64,
    /// CandidateId that received vote in current term (or None)
    pub voted_for: Option<u32>,
}

/// Manages persistence of Raft state to disk
pub struct PersistentStateManager {
    /// Path to the state file
    state_path: PathBuf,
    /// Current state
    state: RaftPersistentState,
}

impl PersistentStateManager {
    /// Creates a new persistent state manager
    ///
    /// # Arguments
    ///
    /// * `base_path` - Base directory for storing state files
    pub fn new(base_path: impl AsRef<Path>) -> io::Result<Self> {
        let base_path = base_path.as_ref();

        // Create directory if it doesn't exist
        if !base_path.exists() {
            fs::create_dir_all(base_path)?;
        }

        let state_path = base_path.join("raft_state");

        // Try to load existing state or create default
        let state = match Self::load_state(&state_path) {
            Ok(state) => {
                info!(
                    "Loaded persistent state: term={}, voted_for={:?}",
                    state.current_term, state.voted_for
                );
                state
            }
            Err(e) => {
                if state_path.exists() {
                    warn!("Failed to load persistent state: {}", e);
                } else {
                    debug!("No existing state file found, using defaults");
                }
                RaftPersistentState::default()
            }
        };

        let manager = Self { state_path, state };

        // Persist initial state to ensure file exists
        manager.persist()?;

        Ok(manager)
    }

    /// Loads state from disk
    fn load_state(path: &Path) -> io::Result<RaftPersistentState> {
        let mut file = File::open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        bincode::deserialize(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Persists current state to disk
    fn persist(&self) -> io::Result<()> {
        let data =
            bincode::serialize(&self.state).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.state_path)?;

        file.write_all(&data)?;
        file.sync_data()?;

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
        Self {
            state_path: PathBuf::from("raft_state"),
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
        manager.update_vote(Some(2))?;

        // Create a new manager to verify persistence
        let manager2 = PersistentStateManager::new(temp_dir.path())?;
        assert_eq!(manager2.get_state().voted_for, Some(2));

        Ok(())
    }
}
