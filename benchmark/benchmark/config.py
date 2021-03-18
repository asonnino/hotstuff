from json import dump, load


class ConfigError(Exception):
    pass


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

class TSSKey:
    def __init__(self, id, name, secret):
        self.id = id
        self.name = name
        self.secret = secret

    @classmethod
    def from_file(cls, filename):
        assert isinstance(filename, str)
        with open(filename, 'r') as f:
            data = load(f)
        return cls(data['id'], data['name'], data['secret'])

class Committee:
    def __init__(self, names, ids, consensus_addr, front_addr, mempool_addr):
        inputs = [names, consensus_addr, front_addr, mempool_addr]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert len({len(x) for x in inputs}) == 1

        self.names = names
        self.ids = ids
        self.consensus = consensus_addr
        self.front = front_addr
        self.mempool = mempool_addr

        self.json = {
            'consensus': self._build_consensus(),
            'mempool': self._build_mempool()
        }

    def _build_consensus(self):
        node = {}
        for a, n, id in zip(self.consensus, self.names, self.ids):
            node[n] = {'name': n, 'stake': 1, 'address': a, 'id': id}
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

    def size(self):
        return len(self.json['consensus']['authorities'])

    @classmethod
    def load(cls, filename):
        assert isinstance(filename, str)
        with open(filename, 'r') as f:
            data = load(f)

        consensus_authorities = data['consensus']['authorities'].values()
        mempool_authorities = data['mempool']['authorities'].values()

        names = [x['name'] for x in consensus_authorities]
        ids = [x['id'] for x in consensus_authorities]
        consensus_addr = [x['address'] for x in consensus_authorities]
        front_addr = [x['front_address'] for x in mempool_authorities]
        mempool_addr = [x['mempool_address'] for x in mempool_authorities]
        return cls(names, ids, consensus_addr, front_addr, mempool_addr)


class LocalCommittee(Committee):
    def __init__(self, names, ids, port):
        assert isinstance(names, list) and all(isinstance(x, str) for x in names)
        assert isinstance(port, int)
        size = len(names)
        consensus = [f'127.0.0.1:{port + i}' for i in range(size)]
        front = [f'127.0.0.1:{port + i + size}' for i in range(size)]
        mempool = [f'127.0.0.1:{port + i + 2*size}' for i in range(size)]
        super().__init__(names, ids, consensus, front, mempool)


class NodeParameters:
    def __init__(self, json):
        inputs = []
        try:
            inputs += [json['consensus']['timeout_delay']]
            inputs += [json['consensus']['sync_retry_delay']]
            inputs += [json['consensus']['max_payload_size']]
            inputs += [json['consensus']['min_block_delay']]
            inputs += [json['consensus']['network_delay']]
            inputs += [json['mempool']['queue_capacity']]
            inputs += [json['mempool']['max_payload_size']]
            inputs += [json['mempool']['min_block_delay']]
            inputs += [json['protocol']]
            inputs += [json['crash']]
        except KeyError as e:
            raise ConfigError(f'Malformed parameters: missing key {e}')

        if not all(isinstance(x, int) for x in inputs):
            raise ConfigError('Invalid parameters type')

        self.timeout_delay = json['consensus']['timeout_delay'] 
        self.network_delay = json['consensus']['network_delay'] 
        self.protocol = json['protocol']
        self.crash = json['crash']
        self.json = json

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'w') as f:
            dump(self.json, f, indent=4, sort_keys=True)


class BenchParameters:
    def __init__(self, json):
        try:
            nodes = json['nodes'] 
            nodes = nodes if isinstance(nodes, list) else [nodes]
            if not nodes:
                raise ConfigError('Missing number of nodes')

            rate = json['rate'] 
            rate = rate if isinstance(rate, list) else [rate]
            if not rate:
                raise ConfigError('Missing input rate')

            self.nodes = [int(x) for x in nodes]
            self.rate = [int(x) for x in rate]
            self.tx_size = int(json['tx_size'])
            self.duration = int(json['duration'])
            self.runs = int(json['runs']) if 'runs' in json else 1
        except KeyError as e:
            raise ConfigError(f'Malformed bench parameters: missing key {e}')

        except ValueError:
            raise ConfigError('Invalid parameters type')

    def result_filename(self, nodes, rate):
        return f'bench-{nodes}-{rate}-{self.tx_size}.txt'
