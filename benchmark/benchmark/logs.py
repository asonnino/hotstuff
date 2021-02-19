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
            with Pool() as p:
                results = p.map(self._parse_clients, clients)
        except (ValueError, IndexError) as e:
            raise ParseError(f'Failed to parse client log: {e}')
        self.txs, self.size, self.rate, self.start, self.end, misses, \
            self.sent_samples = zip(*results)
        self.misses = sum(misses)

        # Parse the nodes logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_nodes, nodes)
        except (ValueError, IndexError) as e:
            raise ParseError(f'Failed to parse node log: {e}')
        self.payload, proposals, commits, samples, timeouts = zip(*results)
        self.proposals = {k: v for x in proposals for k, v in x.items()}
        self.commits = {k: v for x in commits for k, v in x.items()}
        self.samples = {k: v for x in samples for k, v in x.items()}
        self.timeouts = max(timeouts)

        # Check whether clients missed their target rate or if the nodes timed out.
        if self.misses != 0:
            Print.warn(
                f'Clients missed their target rate {self.misses:,} time(s)'
            )

        if self.timeouts > 1:  # It is expected to time out once at the beginning.
            Print.warn(f'Nodes timed out {self.timeouts:,} time(s)')

        # Ensure that all (non-empty) blocks and sample transactions are committed.
        if len(self.proposals) != len(self.commits):
            raise ParseError('Nodes did not commit all non-empty block(s)')

        if any(not x in self.commits for x in self.samples.keys()):
            raise ParseError('Nodes did not commit all sample tx(s)')

    def _verify(self, clients, nodes):
        # Ensure all clients managed to submit their share of txs.
        status = [search(r'Finished', x) for x in clients]
        if sum(x is not None for x in status) != len(clients):
            raise ParseError('Client(s) failed to send all their txs')

        with Pool() as p:
            # Ensure none of the nodes panicked.
            status = p.starmap(search, zip(repeat(r'panic'), nodes))
            if any(x is not None for x in status):
                raise ParseError('Node(s) panicked')

            # Ensure no transactions have been dropped.
            status = p.starmap(search, zip(
                repeat(r'dropping transaction'), nodes)
            )
            if any(x is not None for x in status):
                raise ParseError('Transactions dropped (mempool buffer full)')

    def _parse_clients(self, log):
        txs = int(search(r'Number of transactions: (\d+)', log).group(1))
        size = int(search(r'Transactions size: (\d+)', log).group(1))
        rate = int(search(r'Transactions rate: (\d+)', log).group(1))

        tmp = search(r'\[(.*Z) .* Start ', log).group(1)
        start = self._to_posix(tmp)
        tmp = search(r'\[(.*Z) .* Finished', log).group(1)
        end = self._to_posix(tmp)

        misses = len(findall(r'rate too high', log))

        tmp = findall(r'\[(.*Z) .* sample transaction', log)
        samples = [self._to_posix(x) for x in tmp]

        return txs, size, rate, start, end, misses, samples

    def _parse_nodes(self, log):
        payload = int(search(r'Max payload size: (\d+)', log).group(1))

        tmp = findall(r'\[(.*Z) .* Created B\d+\(([^ ]+)\)', log)
        proposals = {d: self._to_posix(t) for t, d in tmp}

        tmp = findall(r'\[(.*Z) .* Committed B\d+\(([^ ]+)\)', log)
        commits = {d: self._to_posix(t) for t, d in tmp if d in proposals}

        tmp = findall(r'Payload ([^ ]+) contains (\d+) sample', log)
        samples = {d: int(s) for d, s in tmp}

        tmp = findall(r'.* Timeout reached', log)
        timeouts = len(tmp)

        return payload, proposals, commits, samples, timeouts

    def _to_posix(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

    def _consensus_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.proposals.values()), max(self.commits.values())
        duration = end - start
        tps = sum(self.txs) / duration
        bps = tps * self.size[0]
        return tps, bps, duration

    def _consensus_latency(self):
        if not self.commits:
            return 0
        latency = [c - self.proposals[r] for r, c in self.commits.items()]
        return mean(latency)

    def _end_to_end_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.start), max(self.commits.values())
        duration = end - start
        tps = sum(self.txs) / duration
        bps = tps * self.size[0]
        return tps, bps, duration

    def _end_to_end_latency(self):
        start = [x for sub in self.sent_samples for x in sub]
        if not (self.samples and start):
            return 0

        end = []
        for digest, occurrence in self.samples.items():
            time = self.commits[digest]
            end += [time] * occurrence

        return mean(end) - mean(start)

    def result(self):
        consensus_latency = self._consensus_latency() * 1000
        consensus_tps, consensus_bps, _ = self._consensus_throughput()
        end_to_end_tps, end_to_end_bps, duration = self._end_to_end_throughput()
        end_to_end_latency = self._end_to_end_latency() * 1000
        return (
            '\n'
            '-----------------------------------------\n'
            ' RESULTS:\n'
            '-----------------------------------------\n'
            f' Committee size: {self.committee_size} nodes\n'
            f' Number of transactions: {sum(self.txs):,} txs\n'
            f' Max payload size: {self.payload[0]:,} B \n'
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
            f' End-to-end latency: {round(end_to_end_latency):,} ms\n'
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
        ok = isinstance(filenames, list) and filenames \
            and all(isinstance(x, str) for x in filenames)
        if not ok:
            raise ParseError('Invalid input arguments')

        # Load result files.
        self.raw_results = {}
        try:
            for filename in filenames:
                x = int(search(r'\d+', filename).group(0))
                with open(filename, 'r') as f:
                    self.raw_results[x] = f.read()
        except (OSError, ValueError) as e:
            raise ParseError(f'Failed to load logs: {e}')

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

        latency = [int(x) for x in findall(r'End-to-end latency: (\d+)', data)]
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
        try:
            with open(filename, 'w') as f:
                f.write(self.result())
        except OSError as e:
            raise ParseError(f'Failed to print aggregated results: {e}')
