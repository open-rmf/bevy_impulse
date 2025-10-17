/// Builds the frontend and packages it into a tar.gz archive.
/// This requires `pnpm` and all the js dependencies to be available.
///
/// This build script is excluded from the crate, the output tarball is included instead.
/// This allows downstream to build the crate without any of the js stack.

#[cfg(feature = "frontend")]
mod frontend {
    use flate2::{write::GzEncoder, Compression};
    use std::fs::File;
    use std::io::{BufReader, Read};
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use tar::Builder;

    fn build_pnpm<P: AsRef<Path>>(out_dir: P) {
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
        let output_tar_gz_path = out_dir.as_ref().join("dist.tar.gz");

        if Path::new(dist_dir_path).exists() {
            let tar_gz_file =
                File::create(&output_tar_gz_path).expect("Failed to create output tar.gz file");
            let enc = GzEncoder::new(tar_gz_file, Compression::default());
            let mut tar_builder = Builder::new(enc);
            tar_builder.mode(tar::HeaderMode::Deterministic);

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

    pub fn build_frontend() {
        // We put the output in `CARGO_MANIFEST_DIR` instead of `OUT_DIR` because we want to include
        // it in the crate.
        build_pnpm(PathBuf::from(
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
        let current_dist = BufReader::new(current_dist);

        let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR not set");
        build_pnpm(Path::new(&out_dir));

        let new_dist_path = Path::new(&out_dir).join("dist.tar.gz");
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
                println!("cargo::error=Frontend has changed, run `BUILD_FRONTEND=1 cargo build` to update it");
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
