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
use lazy_static::lazy_static;
use magic_crypt::{new_magic_crypt, MagicCryptError, MagicCryptTrait};
use serde::Deserialize;

use std::env;
use std::fs;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub sequence: SequenceConfig,
    pub mysql: MysqlConfig,
    pub redis: RedisConfig,
    pub log: log4rs::file::RawConfig,
}

pub fn decrypt(key: &str, content: &str) -> Result<String, MagicCryptError> {
    let mc = new_magic_crypt!(key, 64);
    mc.decrypt_base64_to_string(content)
}

#[allow(dead_code)]
pub fn encrypt(key: &str, content: &str) -> String {
    let mc = new_magic_crypt!(key, 64);
    mc.encrypt_str_to_base64(content)
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
    // FIXME
    let mut from_genesis = false;
    {
        let mut args = ArgumentParser::new();
        args.refer(&mut file)
            .add_option(&["-c"], Store, "toml config file");
        args.refer(&mut from_genesis)
            .add_option(&["-g"], StoreTrue, "start from genesis");
        args.parse_args_or_exit();
    }
    let mut cfg: Config = toml::from_str(&fs::read_to_string(file)?)?;
    // FIXME
    let opts = mysql::Opts::from_url(&cfg.mysql.url)?;
    let pass = opts
        .get_pass()
        .ok_or_else(|| anyhow::anyhow!("passphrase not exist"))?;
    if pass.starts_with("ENC(") && pass.ends_with(')') {
        let content = pass.trim_start_matches("ENC(").trim_end_matches(')');
        match env::var_os("PBE_KEY") {
            Some(val) => {
                let des = decrypt(val.to_str().unwrap(), content)?;
                let (f, t) = (
                    cfg.mysql
                        .url
                        .find("ENC(")
                        .ok_or_else(|| anyhow::anyhow!("ENC( not found in passphrase"))?,
                    cfg.mysql
                        .url
                        .find(')')
                        .ok_or_else(|| anyhow::anyhow!(") not found in passphrase"))?,
                );
                cfg.mysql.url.replace_range(f..=t, &des);
            }
            None => panic!("$PBE_KEY is not defined in the environment."),
        }
    }
    let log_conf = log4rs::config::Config::builder()
        .appenders(
            cfg.log
                .appenders_lossy(&log4rs::file::Deserializers::default())
                .0,
        )
        .build(cfg.log.root())?;
    log4rs::init_config(log_conf)?;
    Ok(cfg)
}

#[test]
pub fn test_encrypt() {
    let encrypted = encrypt("hello", "root12345678");
    println!("{}", encrypted);
    assert_eq!("root12345678", decrypt("hello", &encrypted).unwrap());
}
