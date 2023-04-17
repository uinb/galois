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
    pub db: String,
    pub bind_addr: String,
}

#[derive(Debug, Parser)]
#[command(author, version)]
pub struct Cli {
    #[arg(short('c'), long("config"), required = true, value_name = "FILE")]
    pub file: std::path::PathBuf,
    #[arg(long)]
    pub skip_decrypt: bool,
}

impl Config {
    fn decrypt(&mut self, key: &str) -> anyhow::Result<()> {
        use magic_crypt::MagicCryptTrait;
        let mc = magic_crypt::new_magic_crypt!(key, 64);
        let dec = mc.decrypt_base64_to_string(&self.prover)?;
        self.prover.replace_range(.., &dec);
        let dec = mc.decrypt_base64_to_string(&self.db)?;
        self.db.replace_range(.., &dec);
        Ok(())
    }

    #[allow(dead_code)]
    fn encrypt(&mut self, key: &str) -> anyhow::Result<()> {
        use magic_crypt::MagicCryptTrait;
        let mc = magic_crypt::new_magic_crypt!(key, 64);
        let enc = mc.encrypt_str_to_base64(&self.prover);
        self.prover.replace_range(.., &enc);
        let enc = mc.encrypt_str_to_base64(&self.db);
        self.db.replace_range(.., &enc);
        Ok(())
    }
}

pub fn init_config_file() -> anyhow::Result<Config> {
    let opts = Cli::parse();
    if opts.skip_decrypt {
        init_config(&std::fs::read_to_string(&opts.file)?, None)
    } else {
        let key = std::env::var_os("MAGIC_KEY").ok_or(anyhow::anyhow!("env MAGIC_KEY not set"))?;
        init_config(
            &std::fs::read_to_string(&opts.file)?,
            key.to_str().map(|s| s.to_string()),
        )
    }
}

fn init_config(toml: &str, key: Option<String>) -> anyhow::Result<Config> {
    let mut cfg: Config = toml::from_str(toml)?;
    if let Some(key) = key {
        cfg.decrypt(&key)?;
    }
    Ok(cfg)
}
