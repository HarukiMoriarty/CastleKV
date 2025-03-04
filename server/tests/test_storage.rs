#[cfg(test)]
mod tests {
    use rpc::gateway::Operation;
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::runtime::Runtime;
    use tokio::sync::mpsc;
    use tokio::time::sleep;

    use server::database::KeyValueDb;
    use server::storage::Storage;

    #[test]
    fn test_database_with_storage() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let dir = tempdir().unwrap();
            let db_path = dir.path();

            let (storage_tx, storage_rx) = mpsc::unbounded_channel();

            // Initialize the database and storage
            let db = KeyValueDb::new(db_path, storage_tx).unwrap();
            let storage = Storage::new(db_path, storage_rx).unwrap();

            // Start the storage service in the background
            let storage_handle = tokio::spawn(storage.run());

            // Test PUT operation
            let put_op = Operation {
                id: 1,
                name: "PUT".to_string(),
                args: vec!["test_key".to_string(), "test_value".to_string()],
            };

            let result = db.execute(&put_op, 1.into());
            assert_eq!(result, "PUT test_key not_found");

            // Flush and wait to ensure data is persisted
            db.sync().await.unwrap();

            // Test GET operation
            let get_op = Operation {
                id: 2,
                name: "GET".to_string(),
                args: vec!["test_key".to_string()],
            };

            let result = db.execute(&get_op, 2.into());
            assert_eq!(result, "GET test_key test_value");

            // Drop the database instance which will drop the sender
            drop(db);

            // Wait for storage to shut down
            storage_handle.await.unwrap();

            // Create a new instance of db and storage to verify persistence
            let (storage_tx2, _) = mpsc::unbounded_channel();
            let db2 = KeyValueDb::new(db_path, storage_tx2).unwrap();

            // Verify the data was persisted to disk and loaded into the new instance
            let get_op2 = Operation {
                id: 3,
                name: "GET".to_string(),
                args: vec!["test_key".to_string()],
            };

            let result = db2.execute(&get_op2, 3.into());
            assert_eq!(result, "GET test_key test_value");
        });
    }

    #[test]
    fn test_batch_operations() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let dir = tempdir().unwrap();
            let db_path = dir.path();

            let (storage_tx, storage_rx) = mpsc::unbounded_channel();

            // Initialize the database and storage with small batch size
            let db = KeyValueDb::new(db_path, storage_tx).unwrap();
            let storage = Storage::new(db_path, storage_rx).unwrap();

            // Start the storage service in the background
            let storage_handle = tokio::spawn(storage.run());

            // Insert multiple items (more than batch size)
            for i in 0..=100 {
                let put_op = Operation {
                    id: i,
                    name: "PUT".to_string(),
                    args: vec![format!("key{}", i), format!("value{}", i)],
                };

                db.execute(&put_op, (i as u64).into());
            }

            // Let the batching process some items (shouldn't need explicit flush)
            sleep(Duration::from_millis(100)).await;

            // Verify some early items
            let get_op = Operation {
                id: 1001,
                name: "GET".to_string(),
                args: vec!["key0".to_string()],
            };

            let result = db.execute(&get_op, 1001.into());
            assert_eq!(result, "GET key0 value0");

            // Force flush for the rest
            db.sync().await.unwrap();

            // Verify last item
            let get_op2 = Operation {
                id: 1002,
                name: "GET".to_string(),
                args: vec!["key9".to_string()],
            };

            let result = db.execute(&get_op2, 1002.into());
            assert_eq!(result, "GET key9 value9");

            // Test SCAN operation
            let scan_op = Operation {
                id: 1003,
                name: "SCAN".to_string(),
                args: vec!["key3".to_string(), "key6".to_string()],
            };

            let result = db.execute(&scan_op, 1003.into());
            assert!(result.contains("key3 value3"));
            assert!(result.contains("key4 value4"));
            assert!(result.contains("key5 value5"));
            assert!(result.contains("key6 value6"));

            // Test DELETE operation
            let delete_op = Operation {
                id: 1004,
                name: "DELETE".to_string(),
                args: vec!["key5".to_string()],
            };

            db.execute(&delete_op, 1004.into());
            db.sync().await.unwrap();

            // Verify deletion
            let get_op3 = Operation {
                id: 1005,
                name: "GET".to_string(),
                args: vec!["key5".to_string()],
            };

            let result = db.execute(&get_op3, 1005.into());
            assert_eq!(result, "GET key5 null");

            // Drop the database instance which will drop the sender
            drop(db);

            // Wait for storage to shut down
            storage_handle.await.unwrap();

            // Verify persistence after restart
            let (storage_tx2, _) = mpsc::unbounded_channel();
            let db2 = KeyValueDb::new(db_path, storage_tx2).unwrap();

            // Check deleted key is still gone
            let get_op4 = Operation {
                id: 1006,
                name: "GET".to_string(),
                args: vec!["key5".to_string()],
            };

            let result = db2.execute(&get_op4, 1006.into());
            assert_eq!(result, "GET key5 null");

            // Check other keys survived
            let get_op5 = Operation {
                id: 1007,
                name: "GET".to_string(),
                args: vec!["key6".to_string()],
            };

            let result = db2.execute(&get_op5, 1007.into());
            assert_eq!(result, "GET key6 value6");
        });
    }
}
