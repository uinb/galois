# Galois
[![License](https://img.shields.io/badge/License-Apache%202.0-orange.svg)](#LICENSE)

## Introduction

Galois is an extremely high performance matching engine written in Rust, typically used for the crypto currency exchange service.

Galois use Event Sourcing pattern to handle tens of thousands of orders per second or event better, depending on the performance of persistence.

Galois is product ready but can not be use directly, you should implement the order/user management known as broker, as well as the crypto coin withdraw/deposition.

## Getting Started

### Dependencies

- MySQL: persist the events and output the match result
- Redis: output the best n price of the orderbook

### Build & Run

```
git clone git@github.com:UINB-Tech/galois.git
cd galois
cargo default nightly
cargo build --release

# init MySQL
mysql -u {user_name} -p {database} < sql/init.sql

# modify the configuration file galois.toml before start
target/release/galois -c galois.toml
```

Galois is now waiting for the incoming events and execute.

### Some of the Instructions

```
mysql-schema：f_id, f_cmd, f_status, f_timestamp

f_cmd(json)
    cmd: u32,    
    order_id: Option<u64>,
    user_id: Option<u64>,    
    base: Option<u32>,
    quote: Option<u32>,
    currency: Option<u32>,
    vol: Option<Decimal>,
    amount: Option<Decimal>,
    price: Option<Decimal>,
    base_precision: Option<u32>,
    quote_precision: Option<u32>,
    taker_fee: Option<Decimal>,
    maker_fee: Option<Decimal>,
    min_amount: Option<Decimal>,
    min_vol: Option<Decimal>,
    enable_market_order: Option<bool>,

ASK_LIMIT | BID_LIMIT =>
    require:            
      base,quote,user_id,order_id,price,amount;
      price/amount is positive;
      order_id exists;
      (base/quote) symbol exsits and open;
       
CANCEL =>
    require:
      base,quote,user_id,order_id;
      order_id exists;
      order_id belongs to user_id;
      (base/quote) symbol exsits and open;
            
TRANSFER_OUT | TRANSFER_IN => 
    require:
      user_id,currency,amount
     
```

## License
Galois is licensed under [Apache 2.0](LICENSE)
