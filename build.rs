// Build script for Rust Connect

use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Tell Cargo to re-run this build script if the proto file changes
    println!("cargo:rerun-if-changed=proto/connector.proto");
    println!("cargo:rerun-if-changed=proto");

    // Get the output directory from cargo
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    // Compile the proto file with file descriptor set for reflection
    tonic_build::configure()
        .build_server(true)
        .file_descriptor_set_path(out_dir.join("connector_descriptor.bin"))
        // Add attribute to suppress clippy warnings for generated code
        .extern_path(".proto", "::proto")
        .type_attribute(".", "#[allow(clippy::enum_variant_names)]")
        .compile_protos(&["proto/connector.proto"], &["proto"])?;

    Ok(())
}
