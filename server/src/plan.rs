use std::collections::{HashMap, HashSet};

use common::{extract_key, form_key, CommandId};
use rpc::gateway::{Command, Operation};

/// A set of keys that are read and written by a command
#[derive(Debug, Clone, Default)]
pub(crate) struct RWSet {
    /// Keys that are read during the command
    pub(crate) read_set: HashSet<String>,
    /// Keys that are written during the command
    pub(crate) write_set: HashSet<String>,
}

/// Execution plan for a database command
#[derive(Debug, Clone)]
pub(crate) struct Plan {
    /// Command identifier
    pub(crate) cmd_id: CommandId,
    /// Associated log entry index for persistence operation (PUT / SWAP / DELETE)
    pub(crate) log_entry_index: Option<u64>,
    /// Operations to be executed
    pub(crate) ops: Vec<Operation>,
    /// Read and write sets
    pub(crate) rw_set: RWSet,
}

impl Plan {
    /// Create a plan from a user command (leader)
    ///
    /// # Arguments
    ///
    /// * `cmd` - The command to convert to a plan
    /// * `partition_info` - Partition information for tables
    pub(crate) fn from_client_command(
        cmd: &Command,
        partition_info: &HashMap<String, (u64, u64)>,
    ) -> Result<Plan, String> {
        assert_ne!(CommandId::INVALID, cmd.cmd_id.into());
        Self::validate_command(cmd, partition_info)?;

        Ok(Plan {
            cmd_id: cmd.cmd_id.into(),
            log_entry_index: None,
            ops: cmd.ops.clone(),
            rw_set: Self::calculate_rw_set(&cmd.ops),
        })
    }

    /// Create a plan from a log entry command (follower)
    ///
    /// # Arguments
    ///
    /// * `cmd` - The command to convert to a plan
    /// * `partition_info` - Partition information for tables
    pub(crate) fn from_log_command(cmd: &Command) -> Result<Plan, String> {
        assert_ne!(CommandId::INVALID, cmd.cmd_id.into());
        Ok(Plan {
            cmd_id: cmd.cmd_id.into(),
            log_entry_index: None,
            ops: cmd.ops.clone(),
            rw_set: Self::calculate_rw_set(&cmd.ops),
        })
    }

    /// Validate that a command is valid for execution
    ///
    /// # Arguments
    ///
    /// * `cmd` - The command to validate
    /// * `partition_info` - Partition information for tables
    fn validate_command(
        cmd: &Command,
        partition_info: &HashMap<String, (u64, u64)>,
    ) -> Result<(), String> {
        if cmd.ops.is_empty() {
            return Err("Command contains no operations".to_string());
        }

        for op in &cmd.ops {
            Self::validate_operation(op, partition_info)?;
        }

        Ok(())
    }

    /// Validate that an operation is valid for execution
    ///
    /// # Arguments
    ///
    /// * `op` - The operation to validate
    /// * `partition_info` - Partition information for tables
    fn validate_operation(
        op: &Operation,
        partition_info: &HashMap<String, (u64, u64)>,
    ) -> Result<(), String> {
        match op.name.to_uppercase().as_str() {
            "PUT" | "SWAP" => {
                if op.args.len() != 2 {
                    return Err(format!(
                        "{} operation requires exactly 2 parameters, got {}",
                        op.name,
                        op.args.len()
                    ));
                }

                Self::validate_key_in_partition(&op.args[0], partition_info)?;
            }
            "GET" | "DELETE" => {
                if op.args.len() != 1 {
                    return Err(format!(
                        "{} operation requires exactly 1 parameter, got {}",
                        op.name,
                        op.args.len()
                    ));
                }

                Self::validate_key_in_partition(&op.args[0], partition_info)?;
            }
            "SCAN" => {
                if op.args.len() != 2 {
                    return Err(format!(
                        "SCAN operation requires exactly 2 parameters, got {}",
                        op.args.len()
                    ));
                }

                // Extract table names and keys
                let (start_table, start_key_num) = extract_key(&op.args[0])?;
                let (end_table, end_key_num) = extract_key(&op.args[1])?;

                // Ensure scan operation is within the same table
                if start_table != end_table {
                    return Err(format!(
                        "SCAN operation must be within the same table. Got '{}' and '{}'",
                        start_table, end_table
                    ));
                }

                // Check if table exists in our partition info
                if let Some(&(partition_start, partition_end)) = partition_info.get(&start_table) {
                    // Check if scan range overlaps with partition range
                    if start_key_num >= partition_end
                        || end_key_num < partition_start
                        || start_key_num < partition_start
                        || end_key_num >= partition_end
                    {
                        return Err(format!(
                            "Scan range [{}, {}] doesn't overlap with partition range [{}, {})",
                            start_key_num, end_key_num, partition_start, partition_end
                        ));
                    }
                } else {
                    return Err(format!(
                        "Table '{}' not found in partition information",
                        start_table
                    ));
                }
            }
            _ => return Err(format!("Unsupported operation: {}", op.name)),
        }

        Ok(())
    }

    /// Helper method to validate a key is within the partition range
    ///
    /// # Arguments
    ///
    /// * `key` - The key to validate
    /// * `partition_info` - Partition information for tables
    fn validate_key_in_partition(
        key: &str,
        partition_info: &HashMap<String, (u64, u64)>,
    ) -> Result<(), String> {
        // Extract table name and key num
        let (table_name, key_num) = extract_key(key)?;

        // Check if table exists in our partition info
        if let Some(&(start_key, end_key)) = partition_info.get(&table_name) {
            // Check if key is within range for this table
            if key_num < start_key || key_num >= end_key {
                return Err(format!(
                    "Key '{}' (table: {}, number: {}) is outside the partition range [{}, {})",
                    key, table_name, key_num, start_key, end_key
                ));
            }
        } else {
            return Err(format!(
                "Table '{}' not found in partition information",
                table_name
            ));
        }

        Ok(())
    }

    /// Calculate read and write sets for a set of operations
    ///
    /// # Arguments
    ///
    /// * `ops` - The operations to analyze
    fn calculate_rw_set(ops: &[Operation]) -> RWSet {
        let mut read_set = HashSet::new();
        let mut write_set = HashSet::new();

        for op in ops {
            match op.name.to_uppercase().as_str() {
                "PUT" | "SWAP" | "DELETE" => {
                    if !op.args.is_empty() {
                        let key = &op.args[0];
                        read_set.remove(key);
                        write_set.insert(key.clone());
                    }
                }
                "GET" => {
                    if !op.args.is_empty() && !write_set.contains(&op.args[0]) {
                        read_set.insert(op.args[0].clone());
                    }
                }
                "SCAN" => {
                    if op.args.len() >= 2 {
                        // Safely unwrap as we've already validated the operation
                        let result = (|| {
                            let (table_name, start_num) = extract_key(&op.args[0])?;
                            let (_, end_num) = extract_key(&op.args[1])?;
                            Ok::<_, String>((table_name, start_num, end_num))
                        })();

                        if let Ok((table_name, start_num, end_num)) = result {
                            for num in start_num..=end_num {
                                let key = form_key(&table_name, num, None);
                                if !write_set.contains(&key) {
                                    read_set.insert(key);
                                }
                            }
                        }
                    }
                }
                _ => unreachable!(), // Skip unsupported operations - they'll be caught by validation
            }
        }

        RWSet {
            read_set,
            write_set,
        }
    }
}
