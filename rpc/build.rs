fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/gateway.proto")?;
    tonic_build::compile_protos("proto/manager.proto")?;
    Ok(())
}
