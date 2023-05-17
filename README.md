[![SAY NO TO THE TRADEMARK POLICY ](https://gist.githubusercontent.com/blyxyas/8f17fbe1cafdeff65bbe6b332d4f4723/raw/715a24df3ad74b838c6b0ff8079d3f7f9172b0db/banner.svg)](https://github.com/blyxyas/no-rust-policy-change)
# Galois

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

Galois works as the prover(a.k.a Proof of Matches) component of [Fusotao](https://github.com/uinb/fusotao). From v0.4.0, we don't support running Galois in standalone mode anymore.
According to the Roadmap 2023, the UINB team is workding on implementing the Proof of Order Relay which enables users to run #[Fusotao Node](https://github.com/uinb/fusotao) as an order relayer(a.k.a broker) rather than supporting multiple prover instance in the network. In the near future, galois will be recoverable from anywhere by pulling the core data and sequences from [Arweave](https://arweave.org/) and under management of the FusoDAO.

## Getting Started

[Fusotao Docs](https://docs.fusotao.org/)

### Dependencies

- MySQL(will be replaced with RocksDB): persist the events and output the match result
- Redis(will be removed): output the best n price of the orderbook

### Quick Start

TODO, or refer to the old version.

### Instructions

TODO, or refer to the old version.

## License
Galois is licensed under [Apache 2.0](LICENSE).
