/*
 * Copyright (C) 2026 Open Source Robotics Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

use std::{
    fs,
    io::{self, Read},
    path::PathBuf,
};

/// The sprite and font assets used by the traffic app are hosted on Gazebo
/// Fuel instead of being stored in the git repository. This build script
/// downloads them into the crate's `assets/` directory on the first build.
const ASSETS_URL: &str = "https://fuel.gazebosim.org/1.0/Open-RMF/models/crossflow_traffic_app_assets/1/crossflow_traffic_app_assets.zip";

/// The files the app loads at runtime. If all of these are present, the
/// download is skipped, so builds are offline-friendly after the first one.
const REQUIRED_FILES: &[&str] = &[
    "fonts/FiraSans-SemiBold.ttf",
    "sprites/cars/car_blue_1.png",
    "sprites/foliage/foliagePack_001.png",
    "sprites/foliage/foliagePack_002.png",
    "sprites/foliage/foliagePack_003.png",
    "sprites/foliage/foliagePack_004.png",
    "sprites/foliage/foliagePack_005.png",
    "sprites/foliage/foliagePack_006.png",
    "sprites/foliage/foliagePack_007.png",
    "sprites/foliage/foliagePack_008.png",
    "sprites/foliage/foliagePack_009.png",
];

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let assets_dir = manifest_dir.join("assets");
    for file in REQUIRED_FILES {
        println!("cargo:rerun-if-changed={}", assets_dir.join(file).display());
    }

    if REQUIRED_FILES
        .iter()
        .all(|file| assets_dir.join(file).exists())
    {
        return;
    }

    println!("cargo:warning=Downloading traffic app assets from {ASSETS_URL}");
    let response = ureq::get(ASSETS_URL).call().unwrap_or_else(|err| {
        panic!(
            "Failed to download the traffic app assets from {ASSETS_URL}: {err}. \
            An internet connection is required for the first build."
        )
    });

    let mut archive_bytes = Vec::new();
    response
        .into_reader()
        .read_to_end(&mut archive_bytes)
        .expect("failed to read the downloaded asset archive");

    let mut archive = zip::ZipArchive::new(io::Cursor::new(archive_bytes))
        .expect("failed to open the downloaded asset archive");
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i).expect("failed to read archive entry");
        if entry.is_dir() {
            continue;
        }
        // Extract only the asset payload; skip Fuel metadata like model.config
        let Some(relative_path) = entry.enclosed_name() else {
            continue;
        };
        if !(relative_path.starts_with("fonts") || relative_path.starts_with("sprites")) {
            continue;
        }

        let out_path = assets_dir.join(relative_path);
        if let Some(parent) = out_path.parent() {
            fs::create_dir_all(parent).expect("failed to create asset directory");
        }
        let mut out_file = fs::File::create(&out_path)
            .unwrap_or_else(|err| panic!("failed to create {}: {err}", out_path.display()));
        io::copy(&mut entry, &mut out_file)
            .unwrap_or_else(|err| panic!("failed to write {}: {err}", out_path.display()));
    }
}
