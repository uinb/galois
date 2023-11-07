// Copyright 2021-2023 UINB Technologies Pte. Ltd.

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

/// dump snapshot at id(executed)
pub fn dump(id: u64, data: &core::Data) {
    if config::C.dry_run.is_some() {
        return;
    }
    let data = data.clone();
    std::thread::spawn(move || -> anyhow::Result<()> {
        let f = std::path::Path::new(&config::C.sequence.get_coredump_path())
            .join(id.to_string())
            .with_extension("gz");
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(f)?;
        data.into_raw(file)?;
        log::info!("snapshot dumped at sequence {}", id);
        Ok(())
    });
}

fn get_id(path: &std::path::Path) -> u64 {
    let file_stem = std::path::Path::new(path.file_stem().unwrap())
        .file_stem()
        .unwrap()
        .to_str()
        .unwrap();
    file_stem.parse::<u64>().unwrap()
}

/// return the id(not executed yet), and the snapshot
pub fn load() -> anyhow::Result<(u64, core::Data)> {
    let dir = std::fs::read_dir(&config::C.sequence.get_coredump_path())?;
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
            let data = core::Data::from_raw(std::fs::File::open(f)?)?;
            print_symbols(&data);
            Ok((event_id + 1, data))
        }
        None => match config::C.sequence.enable_from_genesis {
            true => Ok((1, core::Data::new())),
            false => Err(anyhow::anyhow!(
                "missing snapshot, add `enable_from_genesis` to force to start"
            )),
        },
    }
}

fn print_symbols(data: &core::Data) {
    for k in &data.orderbooks {
        log::info!(
            "base:{}, quote:{}, base_scale:{}, quote_scale: {}, minbase:{}, minquote: {}",
            k.0 .0,
            k.0 .1,
            k.1.base_scale,
            k.1.quote_scale,
            k.1.min_amount,
            k.1.min_vol
        );
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    #[test]
    pub fn test_syspath() {
        let f = Path::new("/tmp/snapshot/")
            .join("2980")
            .with_extension("gz");
        assert_eq!("gz", f.extension().unwrap());
        let filename = Path::new(f.file_stem().unwrap()).file_stem().unwrap();
        assert_eq!("2980", filename);
        assert_eq!("2980", Path::new(filename).file_stem().unwrap());
    }

    #[test]
    pub fn test_max_seq() {
        let f0 = Path::new("/tmp/snapshot/")
            .join("2980")
            .with_extension("gz");
        let f1 = Path::new("/tmp/snapshot/").join("310").with_extension("gz");
        assert_eq!(
            std::cmp::Ordering::Greater,
            super::get_id(&f0).cmp(&super::get_id(&f1))
        );
    }
}
