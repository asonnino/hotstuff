from glob import glob
from re import findall, search
from statistics import mean, stdev
from multiprocessing import Pool
from datetime import datetime
from os.path import join
from itertools import repeat

class ParseError(Exception):
    pass

class LogParser:
    def __init__(self, clients, nodes):
        inputs = [clients, nodes]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        self.committee_size = len(nodes)

        # Parse the clients logs.
        try:
            results = [self._parse_clients(x) for x in clients]
        except (ValueError, IndexError) as e:
            raise ParseError(f'Failed to parse client log: {e}')
        self.txs, self.size, self.rate, self.start, self.end = zip(*results)

        # Parse the nodes logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_nodes, nodes)
        except (ValueError, IndexError) as e:
            raise ParseError(f'Failed to parse node log: {e}')
        proposals, commits = zip(*results)
        self.proposals = {k: v for x in proposals for k, v in x.items()}
        self.commits = {k: v for x in commits for k, v in x.items()}

        # Ensure the benchmark run without errors.
        self._verify(clients, nodes)

    def _verify(self, clients, nodes):
        # Ensure all clients managed to submit their share of txs. 
        status = [search(r'Finished', x) for x in clients]
        if sum(x is not None for x in status) != len(clients):
            raise ParseError('Some clients failed to send all their txs')

        # Ensure no node panicked.
        with Pool() as p:
           status = p.starmap(search, zip(repeat(r'panic'), nodes))
        if any(x is not None for x in status):
            raise ParseError('One or more nodes panicked')

        # Ensure no transactions have been dropped.
        with Pool() as p:
           status = p.starmap(search, zip(repeat(r'Mempool full'), nodes))
        if any(x is not None for x in status):
            raise ParseError('Transactions dropped (mempool buffer full)')

        # Ensure all (non-empty) blocks created are committed.
        if len(self.proposals) != len(self.commits):
            missing = len(self.proposals) - len(self.commits)
            raise ParseError(f'{missing} non-empty blocks have not been committed')

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

        lines = findall(r'.* Created non-empty B\d+', log)
        rounds, times = parse(lines)
        proposals = {r: t for r, t in zip(rounds, times)}

        lines =  findall(r'.* Committed B\d+', log)
        rounds, times = parse(lines)
        commits = {r: t for r, t in zip(rounds, times) if r in proposals}

        return proposals, commits

    def _parse_timestamp(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

    def consensus_throughput(self):
        start, end = min(self.proposals.values()), max(self.commits.values())
        duration = end - start
        tps = sum(self.txs) / duration
        bps = tps * self.size[0]
        return tps, bps

    def consensus_latency(self):
        latency = [c - self.proposals[r] for r, c in self.commits.items()]
        avg = mean(latency) if latency else 0
        std = stdev(latency) if len(latency) > 1 else 0
        return avg, std

    def end_to_end_throughput(self):
        start, end = min(self.start), max(self.commits.values())
        duration = end - start
        tps = sum(self.txs) / duration
        bps = tps * self.size[0]
        return tps, bps

    def print_summary(self):
        consensus_latency = self.consensus_latency()[0] * 1000
        consensus_tps, consensus_bps = self.consensus_throughput()
        end_to_end_tps, end_to_end_bps = self.end_to_end_throughput()
        print(
            '\n'
            '-----------------------------------------\n'
            ' RESULTS:\n'
            '-----------------------------------------\n'
            f' Committee size: {self.committee_size} nodes\n'
            f' Number of transactions: {sum(self.txs):,} txs\n'
            f' Transaction size: {self.size[0]:,} B \n'
            f' Transaction rate: {sum(self.rate):,} tx/s\n'
            '\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' Consensus latency: {round(consensus_latency):,} ms\n'
            '\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
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