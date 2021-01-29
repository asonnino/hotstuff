from json import dump, load


class Key:
    def __init__(self, name, secret):
        self.name = name
        self.secret = secret

    @classmethod
    def from_file(cls, filename):
        assert isinstance(filename, str)
        with open(filename, 'r') as f:
            data = load(f)
        return cls(data['name'], data['secret'])


class Committee:
    def __init__(self, names, consensus_addr, front_addr, mempool_addr):
        inputs = [names, consensus_addr, front_addr, mempool_addr]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert len({len(x) for x in inputs}) == 1

        self.names = names
        self.consensus = consensus_addr
        self.front = front_addr
        self.mempool = mempool_addr

        self.json = {
            'consensus': self._build_consensus(),
            'mempool': self._build_mempool()
        }

    def _build_consensus(self):
        node = {}
        for a, n in zip(self.consensus, self.names):
            node[n] = {'name': n, 'stake': 1, 'address': a}
        return {'authorities': node, 'epoch': 1}

    def _build_mempool(self):
        node = {}
        for n, f, m in zip(self.names, self.front, self.mempool):
            node[n] = {'name': n, 'front_address': f, 'mempool_address': m}
        return {'authorities': node, 'epoch': 1}

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'w') as f:
            dump(self.json, f, indent=4, sort_keys=True)

    def front_addresses(self):
        authorities = self.json['mempool']['authorities']
        return [x['front_address'] for x in authorities.values()]


class LocalCommittee(Committee):
    def __init__(self, names, port):
        size = len(names)
        consensus = [f'127.0.0.1:{port + i}' for i in range(size)]
        front = [f'127.0.0.1:{port + i + size}' for i in range(size)]
        mempool = [f'127.0.0.1:{port + i + 2*size}' for i in range(size)]
        super().__init__(names, consensus, front, mempool)


class Parameters:
    def __init__(self, timeout_delay, sync_retry_delay, queue_capacity, max_payload_size):
        assert isinstance(timeout_delay, int) and timeout_delay > 0
        assert isinstance(sync_retry_delay, int) and sync_retry_delay > 0
        assert isinstance(queue_capacity, int) and queue_capacity > 0
        assert isinstance(max_payload_size, int) and max_payload_size > 0

        self.timeout_delay = timeout_delay
        self.json = {
            'consensus': {
                'timeout_delay': timeout_delay,
                'sync_retry_delay': sync_retry_delay
            },
            'mempool': {
                'queue_capacity': queue_capacity,
                'max_payload_size': max_payload_size
            }
        }

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'w') as f:
            dump(self.json, f, indent=4, sort_keys=True)

    @classmethod
    def default(cls):
        return cls(5000, 10_000, 10_000, 100_000)
