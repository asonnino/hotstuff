> **Note to readers:** This codebase is built from a minimal implementation of the [2-chain variant of the HotStuff consensus protocol by asonnino](https://github.com/asonnino/hotstuff). 

# SuperHotStuff

[![build status](https://img.shields.io/github/workflow/status/AlianBenabdallah/SuperHotStuff/Build/main?style=flat-square&logo=github)](https://github.com/AlianBenabdallah/SuperHotStuff/actions)
[![rustc](https://img.shields.io/badge/rustc-1.64+-blue?style=flat-square&logo=rust)](https://www.rust-lang.org)
[![license](https://img.shields.io/badge/license-Apache-blue.svg?style=flat-square)](LICENSE)

This repo tries to improve the throughput and latency of HotStuff by imposing an arbitrary topology to the mempool. This idea is motivated by the fact that broadcasting a batch of transactions to every peer is not scalable and very costly. Nodes which receive the block will relay it according to the topology. Three different topologies are implemented. :
- `fullmesh` : The broadcaster will send the block to every other nodes.
- `kauri` : The topology is a balanced n-ary tree where the broadcaster is the root.
- `binomial` : The topology is a binomial tree. 

Note that we don't use a custom topology to relay consensus messages as we suppose that these messages are light and therefore are not a bottleneck.

The codebase has been designed to be small, efficient, and easy to benchmark and modify. It has not been designed to run in production but uses real cryptography ([dalek](https://doc.dalek.rs/ed25519_dalek)), networking ([tokio](https://docs.rs/tokio)), and storage ([rocksdb](https://docs.rs/rocksdb)).

## Quick Start

SuperHotStuff is written in Rust, but all benchmarking scripts are written in Python and run with [Fabric](http://www.fabfile.org/).
To deploy and benchmark a testbed of 4 nodes on your local machine, clone the repo and install the python dependencies:

```bash
git clone https://github.com/asonnino/hotstuff.git
cd hotstuff/benchmark
pip install -r requirements.txt
```

You also need to install Clang (required by rocksdb) and [tmux](https://linuxize.com/post/getting-started-with-tmux/#installing-tmux) (which runs all nodes and clients in the background). Finally, run a local benchmark using fabric:

```bash
fab local
```

This command may take a long time the first time you run it (compiling rust code in `release` mode may be slow) and you can customize a number of benchmark parameters in `fabfile.py`. When the benchmark terminates, it displays a summary of the execution similarly to the one below.

```text
-----------------------------------------
 SUMMARY:
-----------------------------------------
 + CONFIG:
 Faults: 0 nodes
 Committee size: 4 nodes
 Input rate: 1,000 tx/s
 Transaction size: 512 B
 Execution time: 20 s

 Consensus timeout delay: 1,000 ms
 Consensus sync retry delay: 10,000 ms
 Mempool GC depth: 50 rounds
 Mempool sync retry delay: 5,000 ms
 Mempool sync retry nodes: 3 nodes
 Mempool batch size: 15,000 B
 Mempool max batch delay: 10 ms

 + RESULTS:
 Consensus TPS: 967 tx/s
 Consensus BPS: 495,294 B/s
 Consensus latency: 2 ms

 End-to-end TPS: 960 tx/s
 End-to-end BPS: 491,519 B/s
 End-to-end latency: 9 ms
-----------------------------------------
```
## Docker benchmark
In order to run a benchmark on [docker](https://www.docker.com/), please follow the steps below: 
- Install docker on every physical machine. 
- Build the docker image on every physical machine with :
    ```
    cd benchmark
    docker build -t superhotstuff .
    ```
- On your main machine, initialize a docker swarm with `docker swarm init`. This command will output a token that you will need in the next step.
- On the other machines, join the swarm with `docker swarm join --token <token> <ip>`.
- Create an overlay network with `docker network create --driver=overlay --subnet=10.1.0.0/16 benchNet`.
- Run `fab docker` on your main machine.

The results will be in the `results` repository following this format : 
`bench-{topology}-{faults}-{nodes}-{clients}-{rate}-{tx_size}-{latency}-{bandwidth}.txt`

## TODO : 
- Signatures for the Ack messages.
- Rewrite the tests to adapt to the new codebase.

## License

This software is licensed as [Apache 2.0](LICENSE).
