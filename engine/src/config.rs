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
#[command(author, version)]
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
#[command(version)]
pub enum SubCmd {
    EncryptConfig,
}

#[derive(Debug, clap::Args)]
#[command(version)]
pub struct RunCmd {
    #[arg(
        long,
        value_name = "EVENT-ID",
        help = "Run galois in `dry-run` mode, skipping all output."
    )]
    dry_run: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub sequence: SequenceConfig,
    pub fusotao: FusotaoConfig,
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
    pub fn get_coredump_path(&self) -> String {
        format!("{}/coredump/", self.data_home)
    }

    pub fn get_storage_path(&self) -> String {
        format!("{}/storage/", self.data_home)
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

#[test]
pub fn test_default() {
    let toml = r#"
        [server]
        bind_addr = "127.0.0.1:8097"
        data_home = "/tmp/galois"

        [sequence]
        checkpoint = 100000
        enable_from_genesis = true

        [fusotao]
        node_url = "ws://localhost:9944"
        key_seed = "//Alice"
        x25519_priv = "0xedcff0c69e4c0fa7e9a36e2e6d07f2cc355c8d25907a0ad2ab7e03b24f8e90f3"
        proof_batch_limit = 20
        claim_block = 1
    "#;
    let config = init_config(&toml, None);
    assert!(config.is_ok())
}
