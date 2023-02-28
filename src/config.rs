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
use lazy_static::lazy_static;
use log4rs::config::{Logger, RawConfig as LogConfig};
use serde::{Deserialize, Serialize};

#[derive(Debug, Parser)]
#[command(author, version, about = r#"
                 **       **
   *******     ******     **               **
  ***               **    **     *****     **    ******
 **              *****    **   ***   ***        **    *
 **            *******    **   **     **   **   **
 **    *****  **    **    **   *       *   **    **
  **     ***  **    **    **   **     **   **     ****
   *********   **  ****   **    *******    **        **
      *    *    ****  *   **      ***      **    ** ***
                                                  ****
"#, long_about = None)]
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
        value_name = "EVENTID",
        help = "Run galois in `dry-run` mode, skipping all output."
    )]
    dry_run: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub sequence: SequenceConfig,
    pub mysql: MysqlConfig,
    pub redis: RedisConfig,
    pub fusotao: FusotaoConfig,
    #[serde(skip_serializing)]
    pub log: LogConfig,
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
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SequenceConfig {
    pub coredump_dir: String,
    pub checkpoint: usize,
    pub batch_size: usize,
    pub dump_mode: String,
    pub fetch_intervel_ms: u64,
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
    pub compress_proofs: bool,
    // deprecated
    pub fee_adjust_threshold: u64,
}

impl Default for FusotaoConfig {
    fn default() -> Self {
        Self {
            node_url: String::from(""),
            key_seed: String::from(""),
            claim_block: 1,
            proof_batch_limit: 20,
            compress_proofs: true,
            fee_adjust_threshold: 10,
        }
    }
}

impl EncryptedConfig for FusotaoConfig {
    fn decrypt(&mut self, key: &str) -> anyhow::Result<()> {
        use magic_crypt::MagicCryptTrait;
        let mc = magic_crypt::new_magic_crypt!(key, 64);
        let dec = mc.decrypt_base64_to_string(&self.key_seed)?;
        self.key_seed.replace_range(.., &dec);
        Ok(())
    }

    fn encrypt(&mut self, key: &str) -> anyhow::Result<()> {
        use magic_crypt::MagicCryptTrait;
        let mc = magic_crypt::new_magic_crypt!(key, 64);
        let enc = mc.encrypt_str_to_base64(&self.key_seed);
        self.key_seed.replace_range(.., &enc);
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

lazy_static! {
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
}

pub fn print_enc_config_file(mut cfg: Config) -> anyhow::Result<()> {
    let key = std::env::var_os("MAGIC_KEY").ok_or(anyhow::anyhow!("env MAGIC_KEY not set"))?;
    let key = key
        .to_str()
        .map(|s| s.to_string())
        .ok_or(anyhow::anyhow!("env MAGIC_KEY not set"))?;
    cfg.mysql.encrypt(&key)?;
    cfg.redis.encrypt(&key)?;
    cfg.fusotao.encrypt(&key)?;
    println!("{}", toml::to_string(&cfg)?);
    Ok(())
}

fn init_config(toml: &str, key: Option<String>) -> anyhow::Result<Config> {
    let mut cfg: Config = toml::from_str(toml)?;
    if let Some(key) = key {
        cfg.mysql.decrypt(&key)?;
        cfg.redis.decrypt(&key)?;
        cfg.fusotao.decrypt(&key)?;
    }
    // TODO replace with env_log
    let mut loggers = cfg
        .log
        .loggers()
        .iter()
        .map(|l| (l.name().to_string(), l.clone()))
        .collect::<std::collections::HashMap<String, _>>();
    loggers
        .entry("ws".to_string())
        .or_insert_with(|| Logger::builder().build("ws".to_string(), log::LevelFilter::Error));
    loggers.entry("ac_node_api".to_string()).or_insert_with(|| {
        Logger::builder().build("ac_node_api".to_string(), log::LevelFilter::Error)
    });
    loggers
        .entry("fusotao_rust_client".to_string())
        .or_insert_with(|| {
            Logger::builder().build("fusotao_rust_client".to_string(), log::LevelFilter::Error)
        });
    let log = log4rs::Config::builder()
        .loggers::<Vec<_>>(loggers.into_values().collect())
        .appenders(cfg.log.appenders_lossy(&Default::default()).0)
        .build(cfg.log.root())?;
    log4rs::init_config(log)?;
    Ok(cfg)
}

#[test]
pub fn test_default() {
    let toml = r#"
        [server]
        bind_addr = "127.0.0.1:8097"
        [mysql]
        url = "mysql://username:password@localhost:3306/galois"
        [redis]
        url = "redis://localhost:6379/0"
        [sequence]
        checkpoint = 100000
        coredump_dir = "/tmp/snapshot"
        batch_size = 1000
        dump_mode = "disk"
        fetch_intervel_ms = 5
        enable_from_genesis = true
        [log]
        [log.appenders.console]
        kind = "console"
        [log.root]
        level = "info"
        appenders = ["console"]
        [fusotao]
        node_url = "ws://localhost:9944"
        key_seed = "//Alice"
        proof_batch_limit = 20
        claim_block = 1
        compress_proofs = true
        fee_adjust_threshold = 1000
    "#;
    let config = init_config(&toml, None).unwrap();
    let mysql_opts = mysql::Opts::from_url(&config.mysql.url).unwrap();
    assert_eq!("password", mysql_opts.get_pass().unwrap());
}
