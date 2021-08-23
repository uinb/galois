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

use argparse::{ArgumentParser, Store, StoreTrue};
use cfg_if::cfg_if;
use lazy_static::lazy_static;
use log4rs::file::RawConfig as LogConfig;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub sequence: SequenceConfig,
    pub mysql: MysqlConfig,
    pub redis: RedisConfig,
    pub log: LogConfig,
    #[cfg(feature = "prover")]
    pub fuso: Option<FusotaoConfig>,
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
}

#[derive(Debug, Deserialize)]
pub struct MysqlConfig {
    pub url: String,
}

#[cfg(feature = "enc-conf")]
impl EncryptedConfig for MysqlConfig {
    fn decrypt(&mut self, key: &str) -> anyhow::Result<()> {
        use magic_crypt::MagicCryptTrait;
        let opts = mysql::Opts::from_url(&self.url)?;
        let (f, t) = (
            self.url
                .find(":")
                .ok_or_else(|| anyhow::anyhow!("invalid mysql config"))?,
            self.url
                .find('@')
                .ok_or_else(|| anyhow::anyhow!("invalid mysql config"))?,
        );
        let mc = magic_crypt::new_magic_crypt!(key, 64);
        let dec = mc.decrypt_base64_to_string(
            opts.get_pass()
                .ok_or_else(|| anyhow::anyhow!("invalid mysql config"))?,
        )?;
        self.url.replace_range(f..=t, &dec);
        Ok(())
    }
}

#[cfg(feature = "prover")]
#[derive(Debug, Deserialize)]
pub struct FusotaoConfig {
    pub node_url: String,
    pub ss58_addr: String,
    pub pri_key: String,
}

#[cfg(feature = "prover, enc-conf")]
impl EncryptedConfig for FusotaoConfig {
    fn decrypt(key: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
pub struct RedisConfig {
    pub url: String,
}

lazy_static! {
    pub static ref C: Config = init_config_file().unwrap();
    pub static ref ENABLE_START_FROM_GENESIS: bool = true;
}

fn init_config_file() -> anyhow::Result<Config> {
    let mut file = String::new();
    let mut from_genesis = false;
    {
        let mut args = ArgumentParser::new();
        args.refer(&mut file)
            .add_option(&["-c"], Store, "toml config file");
        args.refer(&mut from_genesis)
            .add_option(&["-g"], StoreTrue, "start from genesis");
        args.parse_args_or_exit();
    }
    init_config(&std::fs::read_to_string(file)?)
}

fn init_config(toml: &str) -> anyhow::Result<Config> {
    cfg_if! {
        if #[cfg(feature = "enc-conf")] {
            let mut cfg: Config = toml::from_str(toml)?;
            let key = std::env::var_os("MAGIC_KEY")
                .ok_or_else(||anyhow::anyhow!("env MAGIC_KEY not set"))?;
            let key = key.to_str().ok_or_else(||anyhow::anyhow!("env MAGIC_KEY not set"))?;
            cfg.mysql.decrypt(&key)?;
        } else {
            let cfg: Config = toml::from_str(toml)?;
        }
    }
    let log_conf = log4rs::config::Config::builder()
        .appenders(cfg.log.appenders_lossy(&Default::default()).0)
        .build(cfg.log.root())?;
    log4rs::init_config(log_conf)?;
    Ok(cfg)
}

#[test]
#[cfg(not(feature = "enc-conf"))]
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
