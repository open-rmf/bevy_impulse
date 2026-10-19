/// Builds the frontend and packages it into a tar.gz archive.
/// This requires `pnpm` and all the js dependencies to be available.
///
/// This build script is excluded from the crate, the output tarball is included instead.
/// This allows downstream to build the crate without any of the js stack.

#[cfg(feature = "frontend")]
mod frontend {
    use flate2::{Compression, write::GzEncoder};
    use std::fs::File;
    use std::io::{BufReader, Read};
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use tar::Builder;
    use flate2::read::GzDecoder;

    fn build_pnpm<W: std::io::Write>(make_encoding: impl FnOnce() -> W) {
        println!("cargo:rerun-if-changed=package.json");
        println!("cargo:rerun-if-changed=../pnpm-lock.yaml");
        println!("cargo:rerun-if-changed=rsbuild.config.ts");
        println!("cargo:rerun-if-changed=frontend");

        let status = Command::new("pnpm")
            .arg("install")
            .arg("--frozen-lockfile")
            .status()
            .expect("Failed to execute pnpm install");

        if !status.success() {
            panic!("pnpm install failed with status: {:?}", status);
        }

        let status = Command::new("pnpm")
            .arg("build")
            .status()
            .expect("Failed to execute pnpm build");

        if !status.success() {
            panic!("pnpm build failed with status: {:?}", status);
        }

        let dist_dir_path = "dist";

        if Path::new(dist_dir_path).exists() {
            let mut tar_builder = Builder::new(make_encoding());
            tar_builder.mode(tar::HeaderMode::Deterministic);

            let mut paths_to_archive: Vec<PathBuf> = Vec::new();
            for entry in walkdir::WalkDir::new(dist_dir_path) {
                let entry = entry.expect("Failed to read directory entry");
                let path = entry.path();
                if path.is_file() {
                    paths_to_archive.push(path.to_path_buf());
                }
            }
            paths_to_archive.sort_unstable();

            for path in paths_to_archive {
                let relative_path = path
                    .strip_prefix(dist_dir_path)
                    .expect("Failed to strip prefix");
                tar_builder
                    .append_path_with_name(&path, relative_path)
                    .expect("Failed to add file to tar archive");
            }
            tar_builder.finish().expect("Failed to finish tar archive");
            println!(
                "Successfully compressed '{}'",
                dist_dir_path,
            );
        } else {
            panic!(
                "Directory '{}' not found after pnpm build. Frontend build might have failed.",
                dist_dir_path
            );
        }
    }

    /// Used to save a compressed version of the built frontend for caching in
    /// git and embedding into an executor binary so it can be used by downstream
    /// crates without needing any locally installed pnpm machinery.
    fn build_compressed_pnpm<P: AsRef<Path>>(out_dir: P) {
        build_pnpm(
            move || {
                let output_tar_gz_path = out_dir.as_ref().join("dist.tar.gz");
                let tar_gz_file = File::create(output_tar_gz_path).expect("Failed to create output file for dist.tar.gz");
                GzEncoder::new(tar_gz_file, Compression::default())
            }
        );
    }

    /// Use to create an uncompressed version of the built frontend to check it
    /// against the cached blob. This allows us to detect changes to the frontend
    /// that would require us to recompile the crossflow-diagram-editor. We test
    /// against an uncompressed version to avoid the problem reported here:
    /// https://github.com/open-rmf/crossflow/issues/233
    fn build_uncompressed_pnpm<P: AsRef<Path>>(out_dir: P) {
        build_pnpm(
            move || {
                let output_tar_path = out_dir.as_ref().join("dist.tar");
                File::create(output_tar_path).expect("Failed to create output file for dist.tar")
            }
        );
    }

    pub fn build_frontend() {
        // We put the output in `CARGO_MANIFEST_DIR` instead of `OUT_DIR` because we want to include
        // it in the crate.
        build_compressed_pnpm(PathBuf::from(
            std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set"),
        ));
    }

    pub fn verify_frontend() {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
        let current_dist_path = Path::new(&manifest_dir).join("dist.tar.gz");
        let current_dist = File::open(&current_dist_path).expect(&format!(
            "Failed to open {}",
            current_dist_path.to_str().unwrap()
        ));
        let current_dist = BufReader::new(GzDecoder::new(current_dist));

        let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR not set");
        build_uncompressed_pnpm(Path::new(&out_dir));

        let new_dist_path = Path::new(&out_dir).join("dist.tar");
        let new_dist = File::open(&new_dist_path).expect(&format!(
            "Failed to open {}",
            new_dist_path.to_str().unwrap()
        ));
        let new_dist = BufReader::new(new_dist);

        let mut current_bytes = current_dist.bytes();
        let mut new_bytes = new_dist.bytes();
        loop {
            let a = current_bytes.next();
            let b = new_bytes.next();
            if a.is_none() && b.is_none() {
                break;
            }
            if a.is_none() || b.is_none() || a.unwrap().unwrap() != b.unwrap().unwrap() {
                println!(
                    "cargo::error=Frontend has changed, run `BUILD_FRONTEND=1 cargo build` to update it"
                );
                return;
            }
        }
    }
}

fn main() {
    #[cfg(feature = "frontend")]
    {
        println!("cargo::rerun-if-env-changed=BUILD_FRONTEND");
        if std::env::var("BUILD_FRONTEND").is_ok() {
            frontend::build_frontend();
        } else {
            frontend::verify_frontend();
        }
    }
}
