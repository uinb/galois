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

#[derive(Debug, Parser)]
#[command(author = "UINB Tech", version)]
pub struct GaloisCli {
    #[arg(short('c'), long("config"), required = true, value_name = "FILE")]
    pub file: std::path::PathBuf,
    #[arg(long)]
    pub skip_decrypt: bool,
    #[clap(subcommand)]
    pub sub: Option<SubCmd>,
    #[command(flatten)]
    pub run: RunCmd,
}

#[derive(Debug, clap::Subcommand)]
pub enum SubCmd {
    #[clap(
        name = "encrypt",
        about = "Encrypt config file using environment variable MAGIC_KEY as the key"
    )]
    Encrypt,
    #[clap(
        name = "migrate",
        about = "Migrate coredump file and sequence storages"
    )]
    Migrate(MigrateCmd),
}

#[derive(Debug, clap::Args)]
pub struct RunCmd {
    #[arg(
        long,
        value_name = "EVENT_ID",
        help = "Run galois in `dry-run` mode, skipping all outputs."
    )]
    dry_run: Option<u64>,
}

#[derive(Debug, clap::Args)]
pub struct MigrateCmd {
    #[arg(
        long,
        short = 'o',
        value_name = "PATH",
        help = "The new coredump file path"
    )]
    pub output_path: String,
    #[arg(
        long,
        short = 'i',
        value_name = "PATH",
        help = "The old coredump file path"
    )]
    pub input_path: String,
    #[arg(long, action=clap::ArgAction::SetFalse, help = "Migrate coredump file only if set")]
    pub core_only: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub sequence: SequenceConfig,
    pub fusotao: FusotaoConfig,
    #[cfg(feature = "v1-to-v2")]
    pub mysql: MysqlConfig,
    #[serde(skip_serializing)]
    pub dry_run: Option<u64>,
}

pub trait EncryptedConfig {
    fn decrypt(&mut self, key: &str) -> anyhow::Result<()>;
    fn encrypt(&mut self, key: &str) -> anyhow::Result<()>;
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ServerConfig {
    pub bind_addr: String,
    pub data_home: String,
}

impl ServerConfig {
    pub fn get_checkpoint_path(&self) -> String {
        format!("{}/checkpoint/", self.data_home)
    }

    pub fn get_sequence_path(&self) -> String {
        format!("{}/sequence/", self.data_home)
    }

    pub fn get_proof_path(&self) -> String {
        format!("{}/proof/", self.data_home)
    }

    pub fn get_output_path(&self) -> String {
        format!("{}/market/", self.data_home)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SequenceConfig {
    pub checkpoint: u64,
    pub enable_from_genesis: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MysqlConfig {
    pub url: String,
}

impl EncryptedConfig for MysqlConfig {
    fn decrypt(&mut self, key: &str) -> anyhow::Result<()> {
        use magic_crypt::MagicCryptTrait;
        let mc = magic_crypt::new_magic_crypt!(key, 64);
        let dec = mc.decrypt_base64_to_string(&self.url)?;
        self.url.replace_range(.., &dec);
        Ok(())
    }

    fn encrypt(&mut self, key: &str) -> anyhow::Result<()> {
        use magic_crypt::MagicCryptTrait;
        let mc = magic_crypt::new_magic_crypt!(key, 64);
        let enc = mc.encrypt_str_to_base64(&self.url);
        self.url.replace_range(.., &enc);
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct FusotaoConfig {
    pub node_url: String,
    pub key_seed: String,
    pub claim_block: u32,
    pub proof_batch_limit: usize,
    pub x25519_priv: String,
}

impl FusotaoConfig {
    pub fn get_x25519(&self) -> String {
        self.x25519_priv.clone()
    }
}

impl EncryptedConfig for FusotaoConfig {
    fn decrypt(&mut self, key: &str) -> anyhow::Result<()> {
        use magic_crypt::MagicCryptTrait;
        let mc = magic_crypt::new_magic_crypt!(key, 64);
        let dec = mc.decrypt_base64_to_string(&self.key_seed)?;
        self.key_seed.replace_range(.., &dec);
        let dec = mc.decrypt_base64_to_string(&self.x25519_priv)?;
        self.x25519_priv.replace_range(.., &dec);
        Ok(())
    }

    fn encrypt(&mut self, key: &str) -> anyhow::Result<()> {
        use magic_crypt::MagicCryptTrait;
        let mc = magic_crypt::new_magic_crypt!(key, 64);
        let enc = mc.encrypt_str_to_base64(&self.key_seed);
        self.key_seed.replace_range(.., &enc);
        let enc = mc.encrypt_str_to_base64(&self.x25519_priv);
        self.x25519_priv.replace_range(.., &enc);
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RedisConfig {
    pub url: String,
}

impl EncryptedConfig for RedisConfig {
    fn decrypt(&mut self, key: &str) -> anyhow::Result<()> {
        use magic_crypt::MagicCryptTrait;
        let mc = magic_crypt::new_magic_crypt!(key, 64);
        let dec = mc.decrypt_base64_to_string(&self.url)?;
        self.url.replace_range(.., &dec);
        Ok(())
    }

    fn encrypt(&mut self, key: &str) -> anyhow::Result<()> {
        use magic_crypt::MagicCryptTrait;
        let mc = magic_crypt::new_magic_crypt!(key, 64);
        let enc = mc.encrypt_str_to_base64(&self.url);
        self.url.replace_range(.., &enc);
        Ok(())
    }
}

lazy_static::lazy_static! {
    pub static ref C: Config = init_config_file().unwrap();
}

fn init_config_file() -> anyhow::Result<Config> {
    let opts = GaloisCli::parse();
    if opts.skip_decrypt {
        init_config(&std::fs::read_to_string(&opts.file)?, None)
    } else {
        let key = std::env::var_os("MAGIC_KEY").ok_or(anyhow::anyhow!("env MAGIC_KEY not set"))?;
        init_config(
            &std::fs::read_to_string(&opts.file)?,
            key.to_str().map(|s| s.to_string()),
        )
    }
    .map(|mut c| {
        c.dry_run = opts.run.dry_run;
        c
    })
}

pub fn print_config(f: &std::path::PathBuf) -> anyhow::Result<()> {
    let key = std::env::var_os("MAGIC_KEY").ok_or(anyhow::anyhow!("env MAGIC_KEY not set"))?;
    let key = key
        .to_str()
        .map(|s| s.to_string())
        .ok_or(anyhow::anyhow!("env MAGIC_KEY not set"))?;
    let toml = std::fs::read_to_string(f)?;
    let mut cfg: Config = toml::from_str(&toml)?;
    cfg.fusotao.encrypt(&key)?;
    println!("{}", toml::to_string(&cfg)?);
    Ok(())
}

fn init_config(toml: &str, key: Option<String>) -> anyhow::Result<Config> {
    let mut cfg: Config = toml::from_str(toml)?;
    if let Some(key) = key {
        cfg.fusotao.decrypt(&key)?;
    }
    Ok(cfg)
}
