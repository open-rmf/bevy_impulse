fn main() -> std::io::Result<()> {

    #[cfg(feature = "diagram")]
    {
        use std::path::PathBuf;

        let protos = [
            "assets/protos/example_protos/fibonacci.proto",
        ];
        let includes = [
            "assets/protos/",
        ];

        let file_descriptor_path = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR not set"))
            .join("file_descriptor_set.bin");

        prost_build::Config::new()
            .file_descriptor_set_path(file_descriptor_path)
            .compile_protos(&protos, &includes)?;
    }

    Ok(())
}
