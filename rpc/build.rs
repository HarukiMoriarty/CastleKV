fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile_protos(
            &[
                "proto/gateway.proto",
                "proto/raft.proto",
                "proto/manager.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
