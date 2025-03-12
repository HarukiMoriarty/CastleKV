use std::collections::{HashMap, HashSet};

use common::{extract_key, form_key, CommandId};
use rpc::gateway::{Command, Operation};

use crate::log_manager::LogEntry;

#[derive(Debug, Clone, Default)]
pub(crate) struct RWSet {
    pub(crate) read_set: HashSet<String>,
    pub(crate) write_set: HashSet<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct Plan {
    pub(crate) cmd_id: CommandId,
    pub(crate) log_entry_index: Option<u64>,
    pub(crate) ops: Vec<Operation>,
    pub(crate) rw_set: RWSet,
}

impl Plan {
    pub(crate) fn from_command(
        cmd: &Command,
        cmd_id: CommandId,
        partition_info: &HashMap<String, (u64, u64)>,
    ) -> Result<Plan, String> {
        Self::validate_command(cmd, partition_info)?;

        Ok(Plan {
            cmd_id,
            log_entry_index: None,
            ops: cmd.ops.clone(),
            rw_set: Self::calculate_rw_set(&cmd.ops),
        })
    }

    pub(crate) fn from_log_entry(log_entry: LogEntry) -> Result<Plan, String> {
        let mut ops = Vec::new();

        for (index, op) in log_entry.command.ops.into_iter().enumerate() {
            let (name, args) = match op.1.as_str() {
                "NULL" => ("DELETE".to_string(), vec![op.0]),
                _ => ("PUT".to_string(), vec![op.0, op.1]),
            };

            ops.push(Operation {
                id: index as u32,
                name,
                args,
            });
        }

        Ok(Plan {
            cmd_id: 0.into(),
            log_entry_index: Some(log_entry.index),
            ops,
            rw_set: RWSet::default(),
        })
    }

    // Function to validate entire command
    fn validate_command(
        cmd: &Command,
        partition_info: &HashMap<String, (u64, u64)>,
    ) -> Result<(), String> {
        if cmd.ops.is_empty() {
            return Err("Command contains no operations".to_string());
        }

        for op in &cmd.ops {
            Self::validate_operation(op, partition_info)?
        }

        Ok(())
    }

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

                // Extract table name and key num
                let (table_name, key_num) = extract_key(&op.args[0])?;

                // Check if table exists in our partition info
                if let Some(&(start_key, end_key)) = partition_info.get(&table_name) {
                    // Check if key is within range for this table
                    if key_num < start_key || key_num >= end_key {
                        return Err(format!(
                            "Key '{}' (table: {}, number: {}) is outside the partition range [{}, {})",
                            op.args[0], table_name, key_num, start_key, end_key
                        ));
                    }
                } else {
                    return Err(format!(
                        "Table '{}' not found in partition information",
                        table_name
                    ));
                }
            }
            "GET" | "DELETE" => {
                if op.args.len() != 1 {
                    return Err(format!(
                        "{} operation requires exactly 1 parameter, got {}",
                        op.name,
                        op.args.len()
                    ));
                }

                // Extract table name and key
                let (table_name, key_num) = extract_key(&op.args[0])?;

                // Check if table exists in our partition info
                if let Some(&(start_key, end_key)) = partition_info.get(&table_name) {
                    // Check if key is within range for this table
                    if key_num < start_key || key_num >= end_key {
                        return Err(format!(
                            "Key '{}' (table: {}, number: {}) is outside the partition range [{}, {})",
                            op.args[0], table_name, key_num, start_key, end_key
                        ));
                    }
                } else {
                    return Err(format!(
                        "Table '{}' not found in partition information",
                        table_name
                    ));
                }
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

    fn calculate_rw_set(ops: &[Operation]) -> RWSet {
        let mut read_set = HashSet::new();
        let mut write_set = HashSet::new();

        for op in ops {
            match op.name.to_uppercase().as_str() {
                "PUT" | "SWAP" | "DELETE" => {
                    if op.args.len() >= 2 {
                        if read_set.contains(&op.args[0]) {
                            read_set.remove(&op.args[0]);
                        }
                        write_set.insert(op.args[0].clone());
                    }
                }
                "GET" => {
                    if op.args.len() >= 2 && !write_set.contains(&op.args[0]) {
                        read_set.insert(op.args[0].clone());
                    }
                }
                "SCAN" => {
                    if op.args.len() >= 2 {
                        let (table_name, start_num, end_num) = (
                            extract_key(&op.args[0]).unwrap().0,
                            extract_key(&op.args[0]).unwrap().1,
                            extract_key(&op.args[1]).unwrap().1,
                        );
                        for num in start_num..=end_num {
                            if !write_set.contains(&form_key(&table_name, num)) {
                                read_set.insert(form_key(&table_name, num));
                            }
                        }
                    }
                }
                _ => panic!("Unsupported operation: {}", op.name),
            }
        }

        RWSet {
            read_set,
            write_set,
        }
    }
}
