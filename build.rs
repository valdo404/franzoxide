// Removed unused imports

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Tell Cargo to re-run this build script if the proto file changes
    println!("cargo:rerun-if-changed=proto/connector.proto");
    println!("cargo:rerun-if-changed=proto");

    // Compile the proto file
    tonic_build::compile_protos("proto/connector.proto")?;

    Ok(())
}
