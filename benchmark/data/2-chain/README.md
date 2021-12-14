# 2-Chain HotStuff
This folder contains some raw data and plots obtained running a geo-replicated benchmark on AWS as explained in the [wiki](https://github.com/asonnino/hotstuff/wiki), and using the code tagged as [v0.3.0](https://github.com/asonnino/hotstuff/tree/v0.3.0). The section [Interpreting Results](https://github.com/asonnino/hotstuff/wiki/Interpreting-the-Results) of the Wiki provides insights on how to interpret those results.

## Experimental Data
The filename format of raw data is the following:
```
bench-FAULTS-NODES-INPUT_RATE-TX_SIZE.txt
```
where:
- `FAULTS`: The number of faulty (dead) nodes.
- `NODES`: The number of nodes in the testbed.
- `INPUT_RATE`: The total rate at which clients submit transactions to the system.
- `TX_SIZE`: The size of each transactions.

For instance, a file called `bench-3-10-30000-512.txt` indicates it contains results of a benchmark run with 10 nodes, 30,000 input rate, a transaction size of 512B, and 3 faulty nodes.

### Experimental step
The content of our [settings.json](https://github.com/asonnino/hotstuff/blob/main/benchmark/settings.json) file looks as follows:
```json
{
    "testbed": "hotstuff",
    "key": {
        "name": "aws",
        "path": "/absolute/key/path"
    },
    "ports": {
        "consensus": 8000,
        "mempool": 7000,
        "front": 6000
    },
    "repo": {
        "name": "hotstuff",
        "url": "https://github.com/asonnino/hotstuff.git",
        "branch": "main"
    },
    "instances": {
        "type": "m5d.8xlarge",
        "regions": ["us-east-1", "eu-north-1", "ap-southeast-2", "us-west-1", "ap-northeast-1"]
    }
}
```
We set the following `node_params` in our [fabfile](https://github.com/asonnino/hotstuff/blob/main/benchmark/fabfile.py):
```python
node_params = {
    'consensus': {
        'timeout_delay': 5_000,     # ms
        'sync_retry_delay': 5_000,  # ms
    },
    'mempool': {
        'gc_depth': 50,
        'sync_retry_delay': 5_000,  # ms
        'sync_retry_nodes': 3,
        'batch_size': 500_000,      # bytes
        'max_batch_delay': 100      # ms
    }
}
```


