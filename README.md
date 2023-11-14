# Galois

[![License](https://img.shields.io/badge/License-Apache%202.0-orange.svg)](#LICENSE)
[![GitHub Workflow Status (branch)](https://github.com/uinb/galois/actions/workflows/build.yml/badge.svg)](https://github.com/uinb/galois/actions?query=branch%3Amaster)

## Introduction

Galois is an extremely high-performance matching engine written in Rust which uses event-sourcing pattern to handle tens of thousands of orders per second or even better.

The internal structure of Galois looks like below:

```

           sidecar   chain <-+
             ^         |      \
             |         |       \
             v         v        \
   +---->  server   scanner      +
   |          \       /          |
   |\          \     /           |
   | \          \   /            |
   |  +--     sequencer          |
   +              |              |
   |\             |              |
   | \            v              |
   |  +--      executor          |
   |              |              |
   |              |              |
   |              v              |
   +           rocksdb           +
    \           /   \           /
     \         /     \         /
      \       /       \       /
       +-- market committer -+

```

Galois works as the prover of [Fusotao](https://github.com/uinb/fusotao)(a.k.a Proof of Matches). From v0.4, we don't support running Galois in standalone mode anymore.

NOTICE: The v0.7 is still under heavy development.

## Documents

See [Fusotao Docs](https://docs.fusotao.org/).

## How it works

See [Fusotao Greebook](https://www.fusotao.org/fusotao-greenbook.pdf).

## License

[Apache 2.0](LICENSE).
