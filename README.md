# Galois

<p align="center">Granted by<br><img src="https://cryptologos.cc/logos/near-protocol-near-logo.png?v=014" alt="NEAR Grants Program" width="80" height="80"></p>

[![License](https://img.shields.io/badge/License-Apache%202.0-orange.svg)](#LICENSE)
[![GitHub Workflow Status (branch)](https://github.com/uinb/galois/actions/workflows/build.yml/badge.svg)](https://github.com/uinb/galois/actions?query=branch%3Amaster)

  
## Introduction

Galois is an extremely high performance matching engine written in Rust.

Galois uses Event Sourcing pattern to handle tens of thousands of orders per second or even better, depending on the performance of persistence. Basic architecture is shown below.

```
                   core dump(disk)
                        ^
                        ^
                   +----------+
events(mysql)  >>  |  galois  |  >> match results(mysql)/best n price(redis)
                   +----------+
                        ^
                        ^
                 query requests(TCP) 
                       
```

Galois works as the prover(a.k.a Proof of Matches) component of [Fusotao](https://github.com/uinb/fusotao). From v0.4.0, we don't support running Galois in standalone mode anymore. Our goal is to make the singleton prover of Fusotao mainnet recoverable from anywhere by pulling the core data and sequences from [Arweave](https://arweave.org/).

## Getting Started

[Fusotao Docs](https://docs.fusotao.org/)

### Dependencies

- MySQL(will be replaced with RocksDB): persist the events and output the match result
- Redis(will be removed): output the best n price of the orderbook

### Quick Start

TODO

### Instructions

TODO

## License
Galois is licensed under [Apache 2.0](LICENSE)
