use protobuf_codegen_pure::Customize;
use std::path::Path;

struct ProtobufFile {
    pub proto_file: &'static str,
    pub output_dir: &'static str,
}

fn main() -> std::io::Result<()> {
    if !Path::new("coerce").exists() {
        panic!("could not find coerce root directory, please run from the coerce repository root");
    }

    compile_proto(
        vec![
            (
                "coerce/src/protocol/network.proto",
                "coerce/src/remote/net/proto",
            ),
            (
                "coerce/src/protocol/sharding.proto",
                "coerce/src/remote/cluster/sharding/proto",
            ),
            (
                "coerce/src/protocol/persistent/journal.proto",
                "coerce/src/persistent/journal/proto",
            ),
        ]
        .into_iter(),
    );

    Ok(())
}

fn compile_proto<I: Iterator<Item = (&'static str, &'static str)>>(protobuf_files: I) {
    for file in protobuf_files {
        protobuf_codegen_pure::Codegen::new()
            .customize(Customize {
                gen_mod_rs: Some(true),

                ..Default::default()
            })
            .out_dir(file.1)
            .input(file.0)
            .include("coerce/src/protocol/")
            .run()
            .expect(&format!("protoc {}", file.0));
    }
}
