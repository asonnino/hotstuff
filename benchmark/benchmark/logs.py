from datetime import datetime
from glob import glob
from itertools import repeat
from multiprocessing import Pool
from os.path import join
from re import findall, search
from statistics import mean, stdev

from benchmark.utils import Print


class ParseError(Exception):
    pass


class LogParser:
    def __init__(self, clients, nodes):
        inputs = [clients, nodes]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        # Ensure the benchmark run without errors.
        self._verify(clients, nodes)

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

        # Check whether clients missed their target rate.
        status = [findall(r'rate too high', x) for x in clients]
        miss = sum(len(x) for x in status)
        if miss != 0:    
            Print.warn(f'Clients missed their target rate {miss:,} time(s)')

        # Check whether all (non-empty) blocks created are committed.
        if len(self.proposals) != len(self.commits):
            raise ParseError('Nodes did not commit all non-empty block(s)')

    def _verify(self, clients, nodes):
        # Ensure all clients managed to submit their share of txs.
        status = [search(r'Finished', x) for x in clients]
        if sum(x is not None for x in status) != len(clients):
            raise ParseError('Client(s) failed to send all their txs')

        with Pool() as p:
            # Ensure no node panicked.
            status = p.starmap(search, zip(repeat(r'panic'), nodes))
            if any(x is not None for x in status):
                raise ParseError('Node(s) panicked')

            # Ensure no transactions have been dropped.
            status = p.starmap(search, zip(repeat(r'dropping transaction'), nodes))
            if any(x is not None for x in status):
                raise ParseError('Transactions dropped (mempool buffer full)')

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

        lines = findall(r'.* Committed B\d+', log)
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
        return tps, bps, duration

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
        return tps, bps, duration

    def result(self):
        consensus_latency = self.consensus_latency()[0] * 1000
        consensus_tps, consensus_bps, _ = self.consensus_throughput()
        end_to_end_tps, end_to_end_bps, duration = self.end_to_end_throughput()
        return (
            '\n'
            '-----------------------------------------\n'
            ' RESULTS:\n'
            '-----------------------------------------\n'
            f' Committee size: {self.committee_size} nodes\n'
            f' Number of transactions: {sum(self.txs):,} txs\n'
            f' Transaction size: {self.size[0]:,} B \n'
            f' Transaction rate: {sum(self.rate):,} tx/s\n'
            f' Execution time: {round(duration):,} s\n'
            '\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' Consensus latency: {round(consensus_latency):,} ms\n'
            '\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            '-----------------------------------------\n'
        )

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'a') as f:
            f.write(self.result())

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


class LogAggregator:
    def __init__(self, filenames):
        assert isinstance(filenames, list) and filenames
        assert all(isinstance(x, str) for x in filenames)

        # Load result files.
        self.raw_results = {}
        for filename in filenames:
            x = int(search(r'\d+', filename).group(0))
            with open(filename, 'r') as f:
                self.raw_results[x] = f.read()

        # Aggregate results.
        self.aggregated_results = []
        for x, data in sorted(self.raw_results.items()):
            ret = self._aggregate(data)
            mean_tps, std_tps, mean_latency, std_latency = ret
            self.aggregated_results += [(
                f' Variable value: X={x}\n'
                f'  + Average TPS: {round(mean_tps):,} tx/s\n'
                f'  + Std TPS: {round(std_tps):,} tx/s\n'
                f'  + Average latency: {round(mean_latency):,} ms\n'
                f'  + Std latency: {round(std_latency):,} ms\n'
            )]

    def _aggregate(self, data):
        data = data.replace(',', '')

        tps = [int(x) for x in findall(r'End-to-end TPS: (\d+)', data)]
        mean_tps = mean(tps)
        std_tps = stdev(tps) if len(tps) > 1 else 0

        latency = [int(x) for x in findall(r'Consensus latency: (\d+)', data)]
        mean_latency = mean(latency)
        std_latency = stdev(latency) if len(latency) > 1 else 0

        return mean_tps, std_tps, mean_latency, std_latency

    def result(self):
        aggregated_results = '\n'.join(self.aggregated_results)
        raw_results = ''.join(self.raw_results.values())
        return (
            '\n'
            '-----------------------------------------\n'
            ' AGGREGATED RESULTS:\n'
            '-----------------------------------------\n'
            f'{aggregated_results}'
            '-----------------------------------------\n'
            '\n\n\n RAW DATA:\n\n\n'
            f'{raw_results}'
        )

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'w') as f:
            f.write(self.result())