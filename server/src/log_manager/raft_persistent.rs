use serde::{Deserialize, Serialize};
use std::io;
use std::path::Path;
use tracing::{debug, info, warn};

/// Represents the persistent state required by the Raft consensus algorithm
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct RaftPersistentState {
    pub current_term: u64,
    pub voted_for: Option<u32>,
}

/// Manages persistence of Raft state using confy
pub struct PersistentStateManager {
    /// Cached state
    state: RaftPersistentState,
    /// Configuration file name
    config_name: String,
}

impl PersistentStateManager {
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let config_name = path.as_ref().to_string_lossy().into_owned();

        // Load state using confy
        let state: RaftPersistentState =
            match confy::load_path::<RaftPersistentState>(path.as_ref()) {
                Ok(state) => {
                    info!(
                        "Loaded persistent state: term={}, voted_for={:?}",
                        state.current_term, state.voted_for
                    );
                    state
                }
                Err(e) => {
                    warn!("Failed to load persistent state: {}. Using default.", e);
                    RaftPersistentState::default()
                }
            };

        let manager = Self { state, config_name };
        manager.persist()?;

        Ok(manager)
    }

    fn persist(&self) -> io::Result<()> {
        confy::store_path(&self.config_name, self.state)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        debug!(
            "Persisted state: term={}, voted_for={:?}",
            self.state.current_term, self.state.voted_for
        );

        Ok(())
    }

    pub fn get_state(&self) -> RaftPersistentState {
        self.state
    }

    pub fn update_vote(&mut self, candidate_id: Option<u32>) -> io::Result<()> {
        self.state.voted_for = candidate_id;
        self.persist()
    }

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
            state: RaftPersistentState::default(),
            config_name: "./data/state/node".to_string(),
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
        let state_path = temp_dir.path().join("raft_state.toml");

        let mut manager = PersistentStateManager::new(&state_path)?;
        assert_eq!(manager.get_state().current_term, 0);
        assert_eq!(manager.get_state().voted_for, None);

        manager.update_vote(Some(2))?;
        drop(manager);

        let manager2 = PersistentStateManager::new(&state_path)?;
        assert_eq!(manager2.get_state().voted_for, Some(2));

        Ok(())
    }
}
