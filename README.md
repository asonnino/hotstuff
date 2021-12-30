> **Note to readers:** This codebase is useful to get started with BFT consensus and as baseline when designing your own protocols. If you are looking for state-of-the-art BFT protocols, I recommend [Tusk](https://github.com/asonnino/narwhal) (asynchronous) and [Bullshark](https://github.com/asonnino/narwhal/tree/bullshark) (partially-synchronous) that provide superior performance, robustness, and scalability. 

# HotStuff

[![build status](https://img.shields.io/github/workflow/status/asonnino/hotstuff/Rust/main?style=flat-square&logo=github)](https://github.com/asonnino/hotstuff/actions)
[![rustc](https://img.shields.io/badge/rustc-1.48+-blue?style=flat-square&logo=rust)](https://www.rust-lang.org)
[![license](https://img.shields.io/badge/license-Apache-blue.svg?style=flat-square)](LICENSE)

This repo provides a minimal implementation of a [2-chain variant of the HotStuff consensus protocol](https://arxiv.org/abs/2106.10362). The codebase has been designed to be small, efficient, and easy to benchmark and modify. It has not been designed to run in production but uses real cryptography ([dalek](https://doc.dalek.rs/ed25519_dalek)), networking ([tokio](https://docs.rs/tokio)), and storage ([rocksdb](https://docs.rs/rocksdb)).

## Quick Start
HotStuff is written in Rust, but all benchmarking scripts are written in Python and run with [Fabric](http://www.fabfile.org/).
To deploy and benchmark a testbed of 4 nodes on your local machine, clone the repo and install the python dependencies:
```
$ git clone https://github.com/asonnino/hotstuff.git
$ cd hotstuff/benchmark
$ pip install -r requirements.txt
```
You also need to install Clang (required by rocksdb) and [tmux](https://linuxize.com/post/getting-started-with-tmux/#installing-tmux) (which runs all nodes and clients in the background). Finally, run a local benchmark using fabric:
```
$ fab local
```
This command may take a long time the first time you run it (compiling rust code in `release` mode may be slow) and you can customize a number of benchmark parameters in `fabfile.py`. When the benchmark terminates, it displays a summary of the execution similarly to the one below.
```
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

## Next Steps
The [wiki](https://github.com/asonnino/hotstuff/wiki) documents the codebase, explains its architecture and how to read benchmarks' results, and provides a step-by-step tutorial to run [benchmarks on Amazon Web Services](https://github.com/asonnino/hotstuff/wiki/AWS-Benchmarks) accross multiple data centers (WAN).

## License
This software is licensed as [Apache 2.0](LICENSE).
