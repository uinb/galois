# Galois
[![License](https://img.shields.io/badge/License-Apache%202.0-orange.svg)](#LICENSE)
[![GitHub Workflow Status (branch)](https://img.shields.io/github/workflow/status/uinb/galois/Rust%20CI/master)](https://github.com/uinb/galois/actions?query=branch%3Amaster)

## Introduction

Galois is an extremely high performance matching engine written in Rust, typically used for the crypto currency exchange service.

Galois use Event Sourcing pattern to handle tens of thousands of orders per second or even better, depending on the performance of persistence. Basic architecture is shown below.

```
                  core dump(disk)
                       ^
                       ^
                  +----------+
event(mysql)  >>  |  galois  |  >> match results(mysql)/best n price(redis)
                  +----------+
                       ^
                       ^
                query request(TCP) 
                       
```

If you would like to use Galois in your product, you should implement the order/user management known as broker, as well as the blockchain client to handle crypto coin withdraw/deposition.

## Getting Started

### Dependencies

- MySQL: persist the events and output the match result
- Redis: output the best n price of the orderbook

### Quick Start

Download the binary release and extract to any directory you prefer. Then modify the `galois.toml` especially the mysql and redis configurations, as well as the snapshot directory.

```
# init mysql
mysql -u {user_name} -p {database} < init.sql

# start redis
redis-server

galois -c galois.toml
```

Galois is now waiting for the incoming events and execute. Before you can execute orders, you need to issue a new pair and create a mysql table to receive the matching result outputs.

```
# create a table to receive outputs from galois, 100 and 101 represent the base currency code and the quote currency code.
create table t_clearing_result_100_101 like t_clearing_result;
# tell galois to create a new trading pair 101/100 with base_scale=4, quote_scale=4 and other parameters.
insert into t_sequence(f_cmd) values('{"base":101,"quote":100,"base_scale":4,"quote_scale":4,"taker_fee":"0.002","maker_fee":"0.002","min_amount":"0.1","min_vol":"10","enable_market_order":false,"cmd":12}');
# tell galois to open 101/100
insert into t_sequence(f_cmd) values('{"quote":100, "base":101, "cmd":5}');
commit;
```

### Instructions

```
ASK_LIMIT = 0;
BID_LIMIT = 1;
CANCEL = 4;
CANCEL_ALL = 5;
OPEN = 6;
CLOSE = 7;
OPEN_ALL = 8;
CLOSE_ALL = 9;
TRANSFER_OUT = 10;
TRANSFER_IN = 11;
NEW_SYMBOL = 12;
UPDATE_SYMBOL = 13;
QUERY_ORDER = 14;
QUERY_BALANCE = 15;
QUERY_ACCOUNTS = 16;
DUMP = 17;
```

## License
Galois is licensed under [Apache 2.0](LICENSE)
