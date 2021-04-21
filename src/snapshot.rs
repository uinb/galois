// Copyright 2021 UINB Technologies Pte. Ltd.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::{config, core};
use chrono::prelude::DateTime;
use chrono::Utc;
use log;
use std::time::{Duration, UNIX_EPOCH};
use std::{fs, path, thread};

/// dump snapshot at id(executed)
pub fn dump(id: u64, time: u64, data: &core::Data) {
    let data = data.clone();
    let timestamp = UNIX_EPOCH + Duration::from_secs(time);
    let datetime = DateTime::<Utc>::from(timestamp);
    let format = datetime.format("%Y-%m-%dT%H:%M:%S").to_string();
    thread::spawn(move || {
        let f = path::Path::new(&config::C.sequence.coredump_dir)
            .join(id.to_string())
            .with_extension(format!("{}.gz", format));
        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(f)
            .unwrap();
        data.into_raw(file);
        log::info!("snapshot dumped at sequence {}", id);
    });
}

fn get_id(path: &path::Path) -> u64 {
    let file_stem = path::Path::new(path.file_stem().unwrap())
        .file_stem()
        .unwrap()
        .to_str()
        .unwrap();
    file_stem.parse::<u64>().unwrap()
}

/// return the id(not executed yet), and the snapshot
pub fn load() -> anyhow::Result<(u64, core::Data)> {
    let dir = fs::read_dir(&config::C.sequence.coredump_dir)?;
    let file_path = dir
        .map(|e| e.unwrap())
        .filter(|f| f.file_type().unwrap().is_file())
        .map(|e| e.path())
        .filter(|p| p.extension().map_or(false, |s| s == "gz"))
        .max_by(|x, y| get_id(x).cmp(&get_id(y)));
    match file_path {
        Some(f) => {
            let event_id = get_id(&f);
            log::info!(
                "loading snapshot at {}, execute from {}",
                event_id,
                event_id + 1
            );
            Ok((event_id + 1, core::Data::from_raw(fs::File::open(f)?)?))
        }
        None => match *config::ENABLE_START_FROM_GENESIS {
            true => Ok((1, core::Data::new())),
            false => Err(anyhow::anyhow!("missing snapshot, add `-g` to start from genesis")),
        },
    }
}

#[cfg(test)]
mod test {
    use chrono::prelude::DateTime;
    use chrono::Utc;
    use std::path::Path;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    pub fn test() {
        let timestamp = UNIX_EPOCH + Duration::from_secs(1524885322);
        let datetime = DateTime::<Utc>::from(timestamp);
        let format = datetime.format("%Y-%m-%dT%H:%M:%S").to_string();
        let f = Path::new("/tmp/snapshot/")
            .join("2980")
            .with_extension(format)
            .with_extension("gz");
        assert_eq!("gz", f.extension().unwrap());
        let filename = Path::new(f.file_stem().unwrap()).file_stem().unwrap();
        assert_eq!("2980", filename);
        assert_eq!("2980", Path::new(filename).file_stem().unwrap());
    }

    #[test]
    pub fn test_max() {
        let timestamp = UNIX_EPOCH + Duration::from_secs(1524885322);
        let datetime = DateTime::<Utc>::from(timestamp);
        let format = datetime.format("%Y-%m-%dT%H:%M:%S").to_string();
        let f0 = Path::new("/tmp/snapshot/")
            .join("2980")
            .with_extension(&format)
            .with_extension("gz");
        let f1 = Path::new("/tmp/snapshot/")
            .join("310")
            .with_extension(&format)
            .with_extension("gz");
        assert_eq!(
            std::cmp::Ordering::Greater,
            super::get_id(&f0).cmp(&super::get_id(&f1))
        );
    }
}
