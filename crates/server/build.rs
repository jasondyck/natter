//! Descriptors for the LiveKit messages Twirp answers with, compiled from
//! the protos vendored in `proto/` (copied from livekit-protocol 0.8.1) by
//! protox, pure Rust: `livekit::twirp` writes them as LiveKit's protojson.

fn main() {
    println!("cargo:rerun-if-changed=proto");
    let set = protox::Compiler::new(["proto"])
        .and_then(|mut c| {
            c.include_imports(true);
            c.open_files([
                "livekit_room.proto",
                "livekit_agent_dispatch.proto",
                "livekit_egress.proto",
            ])?;
            Ok(c.encode_file_descriptor_set())
        })
        .expect("compiling proto/livekit_room.proto");
    let out = std::path::PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"));
    std::fs::write(out.join("livekit.bin"), set).expect("writing descriptors");
}
