# HotStuff

[![build status](https://img.shields.io/github/workflow/status/asonnino/hotstuff/Build/main?style=flat-square&logo=github)](https://github.com/asonnino/hotstuff/actions)
[![test status](https://img.shields.io/github/workflow/status/asonnino/hotstuff/Tests/main?style=flat-square&logo=github&label=tests)](https://github.com/asonnino/hotstuff/actions)
[![rustc](https://img.shields.io/badge/rustc-1.48+-blue?style=flat-square&logo=rust)](https://www.rust-lang.org)
[![license](https://img.shields.io/badge/license-Apache-blue.svg?style=flat-square)](LICENSE)

This repo provides a minimal implementation of the HotStuff consensus protocol. The codebase has been designed to be small, efficient, and easy to benchmark and modify. It has not been designed to run in production but uses real cryptography ([dalek](https://doc.dalek.rs/ed25519_dalek)), networking ([tokio](https://docs.rs/tokio)), and storage ([rocksdb](https://docs.rs/rocksdb)).

## Quick Start
HotStuff is written in Rust, but all benchmarking scripts are written in Python and run with [Fabric](http://www.fabfile.org/).
To deploy and benchmark a testbed of 4 nodes on your local machine, clone the repo and install the python dependencies:
```
$ git clone https://github.com/asonnino/hotstuff.git
$ cd hotstuff/benchmark
$ pip install -r requirements.txt
```
You also need to [install tmux](https://linuxize.com/post/getting-started-with-tmux), which runs all nodes and clients in the background.
Finally, run a local benchmark using fabric:
```
$ fab local
```

## License
This software is licensed as [Apache 2.0](LICENSE).
