use std::env;
use std::fs;

use protobuf_codegen_pure::Customize;
use std::path::Path;

fn main() -> std::io::Result<()> {
    if !Path::new("coerce").exists() {
        panic!("could not find coerce root directory, please run from the coerce repository root");
    }

    protobuf_codegen_pure::Codegen::new()
        .customize(Customize {
            gen_mod_rs: Some(true),
            ..Default::default()
        })
        .out_dir("coerce/src/remote/net/proto")
        .input("coerce/src/remote/net/proto/protocol.proto")
        .include("coerce/src/remote/net/proto")
        .run()
        .expect("protoc");

    Ok(())
}
