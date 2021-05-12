This folder contains some raw data and plots obtained running a geo-replicated benchmark on AWS as explained in the [wiki](https://github.com/asonnino/hotstuff/wiki). The section [Interpreting Results](https://github.com/asonnino/hotstuff/wiki/Interpreting-the-Results) of the Wiki provides insights on how to interpret those results.

The filename format of raw data is the following:
```
bench-NODES-INPUT_RATE-TX_SIZE-FAULTS.txt
```
where:
- `NODES`: The number of nodes in the testbed.
- `INPUT_RATE`: The total rate at which clients submit transactions to the system.
- `TX_SIZE`: The size of each transactions.
- `FAULTS`: The number of faulty (dead) nodes.

For instnace, a file called `bench-10-30000-512-3.txt` indicates it contains results of a benchmark run with 10 nodes, 30,000 input rate, a transaction size of 512B, and 3 faulty nodes.
