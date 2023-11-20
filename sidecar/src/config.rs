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

use clap::Parser;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub prover: String,
    pub db_dir: String,
    pub bind_addr: String,
}

#[derive(Debug, Parser)]
#[command(author, version)]
pub struct Cli {
    #[arg(short('c'), long("config"), required = true, value_name = "FILE")]
    pub file: std::path::PathBuf,
}

pub fn init_config_file() -> anyhow::Result<Config> {
    let opts = Cli::parse();
    init_config(&std::fs::read_to_string(&opts.file)?)
}

fn init_config(toml: &str) -> anyhow::Result<Config> {
    let cfg: Config = toml::from_str(toml)?;
    Ok(cfg)
}
