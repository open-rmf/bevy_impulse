/// Builds the frontend and packages it into a tar.gz archive.
/// This requires `pnpm` and all the js dependencies to be available.
///
/// This build script is excluded from the crate, the output tarball is included instead.
/// This allows downstream to build the crate without any of the js stack.

#[cfg(feature = "frontend")]
mod frontend {
    use flate2::{write::GzEncoder, Compression};
    use std::fs::File;
    use std::path::PathBuf;
    use std::process::Command;
    use tar::Builder;

    pub fn build_frontend() {
        println!("cargo:rerun-if-changed=build.rs");
        println!("cargo:rerun-if-changed=package.json");
        println!("cargo:rerun-if-changed=pnpm-lock.yaml");
        println!("cargo:rerun-if-changed=rsbuild.config.ts");
        println!("cargo:rerun-if-changed=frontend");

        let status = Command::new("pnpm")
            .arg("build")
            .status()
            .expect("Failed to execute pnpm build");

        if !status.success() {
            panic!("pnpm build failed with status: {:?}", status);
        }

        let dist_dir_path = "dist";
        // We put the output in `CARGO_MANIFEST_DIR` instead of `OUT_DIR` because we want to include
        // it in the crate.
        let out_dir =
            PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set"));
        let output_tar_gz_path = out_dir.join("dist.tar.gz");

        if std::path::Path::new(dist_dir_path).exists() {
            let tar_gz_file = File::create(&output_tar_gz_path)
                .expect("Failed to create output tar.gz file in CARGO_MANIFEST_DIR");
            let enc = GzEncoder::new(tar_gz_file, Compression::default());
            let mut tar_builder = Builder::new(enc);

            // Add the entire "dist" directory to the archive, preserving its name within the archive.
            tar_builder
                .append_dir_all(".", dist_dir_path)
                .expect("Failed to add directory to tar archive");
            tar_builder.finish().expect("Failed to finish tar archive");
            println!(
                "Successfully compressed '{}' into '{:?}'",
                dist_dir_path, output_tar_gz_path
            );
        } else {
            panic!(
                "Directory '{}' not found after pnpm build. Frontend build might have failed.",
                dist_dir_path
            );
        }
    }
}

fn main() {
    #[cfg(feature = "frontend")]
    frontend::build_frontend();
}
