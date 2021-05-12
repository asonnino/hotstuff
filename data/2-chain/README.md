This folder contains benchmark data for the tagged as [v0.2.4](https://github.com/asonnino/hotstuff/tree/v0.2.4).

### Experimental step
The content of our [settings.json](https://github.com/asonnino/hotstuff/blob/main/benchmark/settings.json) file looks as follows:
```json
{
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
When benchmarking the code base with not faulty nodes, we set the following `node_params` in our [fabfile](https://github.com/asonnino/hotstuff/blob/main/benchmark/fabfile.py):
```python
node_params = {
    'consensus': {
        'timeout_delay': 30_000,
        'sync_retry_delay': 100_000,
        'max_payload_size': 1_000,
        'min_block_delay': 100
    },
    'mempool': {
        'queue_capacity': 100_000,
        'sync_retry_delay': 100_000,
        'max_payload_size': 500_000,
        'min_block_delay': 100
    }
}
```
We then decrease the consensus timeout delay (`'timeout_delay`) to 5 seconds when benchmarking with faults.
