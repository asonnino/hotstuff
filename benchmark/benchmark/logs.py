from glob import glob
from re import findall, search
from statistics import mean, stdev
from multiprocessing import Pool
from datetime import datetime
from os.path import join

class ParseError(Exception):
    pass

class LogParser:
    def __init__(self, clients, nodes):
        inputs = [clients, nodes]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        self.committee_size = len(nodes)

        # Ensure all clients managed to submit their share of txs. 
        if not self._verify_completion(clients):
            raise ParseError('Some clients failed to send all their txs')

        # Parse the clients logs.
        try:
            results = [self._parse_clients(x) for x in clients]
        except (ValueError, IndexError) as e:
            raise ParseError(f'Failed to parse client log: {e}')
        self.txs, self.size, self.rate, self.start, self.end = zip(*results)

        # Parse the nodes logs.
        p = Pool()
        try:
            results = p.map(self._parse_nodes, nodes)
        except (ValueError, IndexError) as e:
            raise ParseError(f'Failed to parse node log: {e}')
        p.close()
        self.proposals, self.commits = zip(*results)

    def _verify_completion(self, clients):
        status = [search(r'Finished', x) for x in clients]
        return sum([x is not None for x in status]) == len(clients)

    def _parse_clients(self, log):
        txs = int(search(r'(Number of transactions:) (\d+)', log).group(2))
        size = int(search(r'(Transactions size:) (\d+)', log).group(2))
        rate = int(search(r'(Transactions rate:) (\d+)', log).group(2))

        tmp = search(r'([-:.T0123456789]*Z) (.*) (Start)', log).group(1)
        start = self._parse_timestamp(tmp)
        tmp = search(r'([-:.T0123456789]*Z) (.*) (Finished)', log).group(1)
        end = self._parse_timestamp(tmp)
    
        return txs, size, rate, start, end

    def _parse_nodes(self, log):
        def parse(lines): 
            rounds = [int(search(r'(B)(\d+)', x).group(2)) for x in lines]
            times = [search(r'[-:.T0123456789]*Z', x).group(0) for x in lines]
            times = [self._parse_timestamp(x) for x in times]
            return rounds, times

        lines = findall(r'.* Created B\d+', log)
        rounds, times = parse(lines)
        proposals = {r: t for r, t in zip(rounds, times)}

        lines =  findall(r'.* Committed B\d+', log)
        rounds, times = parse(lines)
        commits = {r: t for r, t in zip(rounds, times) if r in proposals}

        return proposals, commits

    def _parse_timestamp(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

    def latency(self):
        proposals = {k: v for x in self.proposals for k, v in x.items()}
        commits = {k: v for x in self.commits for k, v in x.items()}
        latency = [c - proposals[r] for r, c in commits.items()]
        avg = mean(latency) if latency else 0
        std = stdev(latency) if len(latency) > 1 else 0
        return avg, std

    def print_summary(self):
        print(
            '\n'
            '-----------------------------------------\n'
            ' RESULTS:\n'
            '-----------------------------------------\n'
            f' Committee size: {self.committee_size} nodes\n'
            f' Number of transactions: {self.txs[0]:,} txs\n'
            f' Transaction size: {self.size[0]:,} B \n'
            f' Transaction rate: {self.rate[0]:,} tx/s\n'
            '\n'
            f' TPS: {0} tx/s\n'
            f' BPS: {0} B/s\n'
            f' Block latency: {round(self.latency()[0] * 1000)} ms\n'
            '-----------------------------------------\n'
        )

    @classmethod
    def process(cls, directory):
        assert isinstance(directory, str)

        clients = []
        for filename in glob(join(directory, 'client-*.log')):
            with open(filename, 'r') as f:
                clients += [f.read()]
        nodes = []
        for filename in glob(join(directory, 'node-*.log')):
            with open(filename, 'r') as f:
                nodes += [f.read()]

        return cls(clients, nodes)


if __name__ == '__main__':
    import sys
    LogParser.process(sys.argv[1]).print_summary()