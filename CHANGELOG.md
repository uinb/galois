# v0.7.0

- remove redis & mysql
- add rocksdb
- add migration support from v0.6.x to v0.7.x
- rewrite sequencer & order manager: read and write events/orders from/to rocksdb
- rewrite prover: read and write proofs from/to rocksdb

# v0.7.0-rc.13

- online test of migration

# v0.6.2

- initiate dump command from external storage to support migration

# v0.5.1

- add polygon chain id
- extend fusotao events scanning procedure to support markets/x25519_key
- remove mmap
- seperate some shared data from core data
- add broker signature support
- encrypt config file
- merge sidecar

# v0.4.1

- reduce output flush delay

# ~v0.4.0~

(don't use this version)
- add: verify compress
- add: some refactor
- remove: unused features
- update rpc client
- remove some features
- refactor some code

# v0.3.8

- upgrade substrate version to 0.9.30
- compress proofs

# v0.3.4-test.1

- verify compressed proofs

# v0.3.3

- release 0.3.3

# v0.3.1

- release 0.3.1, see the previous dozens of RELEASE CANDIDATES

# v0.3.1-rc.13
- reduce logs in loop
- update submit proof
- disable updating fees when PPI is too high

# v0.3.1-test.2
- update submit proof

# v0.3.1-rc.10
- update proof-submitting procedure
- refactor scanning procedure

# v0.3.1-rc.4
- update redis tls config

# v0.3.1-rc.2
- first prd rc

# v0.3.1-rc.1
- first prd rc

# v0.2.6-rc.8
- fixbug about submitting proofs

# v0.2.6-rc.7
- update dominator status

# v0.2.6-rc.6
- fixbug: force update proved_event_id from chain fixed time

# v0.2.6-rc.5
- fixbug: sync proved event id from chain when submit proofs failed

# v0.2.6-rc.3
- fixbug: force set scale of transfer_in/out to 18

# v0.2.6-rc.2

- enable rejecting authorizing

# v0.2.5-rc.5

- wait for proofs InBlock

# v0.2.4

- MaxFee Constraint of increasing fee

# v0.2.3

- add Proving Performance Index(PPI)

# v0.2.2

- ignore unchanged merkle root

# v0.2.1

- merge makers in a single proof

# v0.2.0

- add self-trade prevention

# v0.1.3

- synchronizing blocks concurrently

# v0.1.2

- attach config example

# v0.1.1

- first release
- fusotao master compatible
