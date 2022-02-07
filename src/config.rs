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

use argparse::{ArgumentParser, Store};
use cfg_if::cfg_if;
use lazy_static::lazy_static;
use log4rs::config::{Logger, RawConfig as LogConfig};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub sequence: SequenceConfig,
    pub mysql: MysqlConfig,
    pub redis: RedisConfig,
    pub log: LogConfig,
    pub fusotao: Option<FusotaoConfig>,
}

#[cfg(feature = "enc-conf")]
pub trait EncryptedConfig {
    fn decrypt(&mut self, key: &str) -> anyhow::Result<()>;
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub bind_addr: String,
}

#[derive(Debug, Deserialize)]
pub struct SequenceConfig {
    pub coredump_dir: String,
    pub checkpoint: usize,
    pub batch_size: usize,
    pub dump_mode: String,
    pub fetch_intervel_ms: u64,
    pub enable_from_genesis: bool,
}

#[derive(Debug, Deserialize)]
pub struct MysqlConfig {
    pub url: String,
}

#[cfg(feature = "enc-conf")]
impl EncryptedConfig for MysqlConfig {
    fn decrypt(&mut self, key: &str) -> anyhow::Result<()> {
        use magic_crypt::MagicCryptTrait;
        let mc = magic_crypt::new_magic_crypt!(key, 64);
        let dec = mc.decrypt_base64_to_string(&self.url)?;
        self.url.replace_range(.., &dec);
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
pub struct FusotaoConfig {
    pub node_url: String,
    pub key_seed: String,
    pub claim_block: u32,
    pub fee_adjust_threshold: u64,
}

impl Default for FusotaoConfig {
    fn default() -> Self {
        Self {
            node_url: String::from(""),
            key_seed: String::from(""),
            claim_block: 1,
            fee_adjust_threshold: 10,
        }
    }
}

#[cfg(feature = "enc-conf")]
impl EncryptedConfig for FusotaoConfig {
    fn decrypt(&mut self, key: &str) -> anyhow::Result<()> {
        use magic_crypt::MagicCryptTrait;
        let mc = magic_crypt::new_magic_crypt!(key, 64);
        let dec = mc.decrypt_base64_to_string(&self.key_seed)?;
        self.key_seed.replace_range(.., &dec);
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
pub struct RedisConfig {
    pub url: String,
}

lazy_static! {
    pub static ref C: Config = init_config_file().unwrap();
}

fn init_config_file() -> anyhow::Result<Config> {
    let mut file = String::new();
    {
        let mut args = ArgumentParser::new();
        args.refer(&mut file)
            .add_option(&["-c"], Store, "toml config file");
        args.parse_args_or_exit();
    }
    init_config(&std::fs::read_to_string(file)?)
}

fn init_config(toml: &str) -> anyhow::Result<Config> {
    cfg_if! {
        if #[cfg(feature = "enc-conf")] {
            let mut cfg: Config = toml::from_str(toml)?;
            let key = std::env::var_os("MAGIC_KEY")
                .ok_or(anyhow::anyhow!("env MAGIC_KEY not set"))?;
            let key = key.to_str().ok_or_else(||anyhow::anyhow!("env MAGIC_KEY not set"))?;
            cfg.mysql.decrypt(&key)?;
            if let Some(ref mut fuso) = cfg.fusotao {
                fuso.decrypt(&key)?;
            }
        } else {
            let cfg: Config = toml::from_str(toml)?;
        }
    }
    let mut loggers = cfg
        .log
        .loggers()
        .iter()
        .map(|l| (l.name().to_string(), l.clone()))
        .collect::<std::collections::HashMap<String, _>>();
    loggers
        .entry("ws".to_string())
        .or_insert_with(|| Logger::builder().build("ws".to_string(), log::LevelFilter::Error));
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
#[cfg(not(feature = "fusotao"))]
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
"#;
    let config = init_config(&toml).unwrap();
    let mysql_opts = mysql::Opts::from_url(&config.mysql.url).unwrap();
    assert_eq!("password", mysql_opts.get_pass().unwrap());
}
