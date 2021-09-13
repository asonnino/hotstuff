from datetime import datetime
from multiprocessing import Pool
from os.path import join
from re import findall, search
from statistics import mean
from re import findall, search
from typing import DefaultDict
import matplotlib.pyplot as plt
from glob import glob
from itertools import cycle
import matplotlib.ticker as ticker
from collections import defaultdict


class Parser:
    def __init__(self, clients, nodes):
        # Parse the clients logs.
        with Pool() as p:
            results = p.map(self._parse_clients, clients)
        self.tx_size, self.start, self.sent_samples = zip(*results)

        # Parse the nodes logs.
        with Pool() as p:
            results = p.map(self._parse_nodes, nodes)
        proposals, commits, sizes, self.received_samples = zip(*results)
        self.proposals = self._merge_results([x.items() for x in proposals])
        self.commits = self._merge_results([x.items() for x in commits])
        self.sizes = {
            k: v for x in sizes for k, v in x.items() if k in self.commits
        }

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if not k in merged or merged[k] > v:
                    merged[k] = v
        return merged

    def _parse_clients(self, log):
        tx_size = int(search(r'Transactions size: (\d+)', log).group(1))

        start = self._to_posix(search(r'\[(.*Z) .* Start ', log).group(1))

        tmp = findall(r'\[(.*Z) .* sample transaction (\d+)', log)
        samples = {int(s): self._to_posix(t) for t, s in tmp}

        return tx_size, start, samples

    def _parse_nodes(self, log):
        tmp = findall(r'\[(.*Z) .* Created B\d+\(([^ ]+)\)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        proposals = self._merge_results([tmp])

        tmp = findall(r'\[(.*Z) .* Committed B\d+\(([^ ]+)\)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        commits = self._merge_results([tmp])

        tmp = findall(r'Payload ([^ ]+) contains (\d+) B', log)
        sizes = {d: int(s) for d, s in tmp}

        tmp = findall(r'Payload ([^ ]+) contains sample tx (\d+)', log)
        samples = {int(s): d for d, s in tmp}

        return proposals, commits, sizes, samples

    def _to_posix(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

    def throughput(self, interval):
        start = round(min(self.start))
        end = round(max(self.commits.values())) - interval
        plot_data = {}  # Dictionary of interval: tps
        for i, t in enumerate(range(start, end, interval)):
            relevant_blocks = {
                k: v for k, v in self.commits.items() if v >= t and v < t+interval
            }
            relevant_blocks_size = {
                k: s for k, s in self.sizes.items() if k in relevant_blocks
            }
            bytes = sum(relevant_blocks_size.values())
            bps = bytes / interval
            tps = bps / self.tx_size[0]

            x = interval * (i + 1/2)
            plot_data[x] = tps

        return plot_data

    def latency(self, interval):
        start = round(min(self.start))
        end = round(max(self.commits.values())) - interval
        plot_data = defaultdict(list)  # Dictionary of interval: latencies
        for i, t in enumerate(range(start, end, interval)):
            for sent, received in zip(self.sent_samples, self.received_samples):
                for tx_id, batch_id in received.items():
                    assert tx_id in sent  # We receive txs that we sent.
                    if batch_id in self.commits:
                        start = sent[tx_id]
                        end = self.commits[batch_id]
                        if end >= t and end < t + interval:
                            start = sent[tx_id]

                            x = interval * (i + 1/2)
                            plot_data[x] += [end-start]

        return {k: mean(v) for k, v in plot_data.items()}

    @ classmethod
    def process(cls, directory):
        assert isinstance(directory, str)

        clients = []
        for filename in sorted(glob(join(directory, 'client-*.log'))):
            with open(filename, 'r') as f:
                clients += [f.read()]
        nodes = []
        for filename in sorted(glob(join(directory, 'node-*.log'))):
            with open(filename, 'r') as f:
                nodes += [f.read()]

        return cls(clients, nodes)


@ ticker.FuncFormatter
def default_major_formatter(x, pos):
    if x >= 1_000:
        return f'{x/1000:.0f}k'
    else:
        return f'{x:.0f}'


class Ploter:
    @ staticmethod
    def plot_time(round_robin, reputation, y_label, plot_type, fig_size=(12.8, 4.8), legend_anchor=(0.5, 1)):
        plt.figure(figsize=fig_size)
        markers = cycle(['o', 'v', 's', 'd'])
        styles = cycle(['dashed', 'dotted'])
        colors = cycle(['tab:orange', 'tab:blue', 'tab:green'])

        plt.axvline(x=60, color='k', linewidth=1)
        plt.axvline(x=120, color='k', linewidth=1)
        plt.axvline(x=180, color='k', linewidth=1)

        for scheme in [(round_robin, 'Round-Robin'), (reputation, 'Carousel')]:
            x, name = scheme
            style = next(styles)
            marker = next(markers)
            color = next(colors)
            plt.errorbar(
                x.keys(), x.values(), label=f'{name}, 10 nodes (3 faulty)',
                linestyle=style, marker=marker, color=color, capsize=3, linewidth=2
            )

        plt.xlabel('Time (s)', fontweight='bold')
        plt.ylabel(y_label, fontweight='bold')
        plt.xticks(weight='bold')
        plt.yticks(weight='bold')
        ax = plt.gca()
        ax.yaxis.set_major_formatter(default_major_formatter)
        ax.xaxis.set_major_locator(plt.MaxNLocator(10))
        plt.legend(loc='upper center', bbox_to_anchor=legend_anchor, ncol=1)
        plt.xlim(xmin=0)
        plt.ylim(bottom=0)
        plt.grid(True)

        for x in ['pdf', 'png']:
            plt.savefig(f'{plot_type}.{x}', bbox_inches='tight')


if __name__ == '__main__':
    interval = 10

    print('Parsing round-robin logs...')
    parser = Parser.process('round-robin-logs')
    round_robin_tps = parser.throughput(interval)
    round_robin_latency = parser.latency(interval)

    print('Parsing leader-reputation logs...')
    parser = Parser.process('leader-reputation-logs')
    leader_reputation_tps = parser.throughput(interval)
    leader_reputation_latency = parser.latency(interval)

    print('Making plots...')
    Ploter.plot_time(
        round_robin_tps,
        leader_reputation_tps,
        'Throughput (cmd/s)',
        'time_tps',
        legend_anchor=(0.85, 1)
    )
    Ploter.plot_time(
        round_robin_latency,
        leader_reputation_latency,
        'Latency (s)',
        'time_latency',
        legend_anchor=(0.15, 1)
    )
    print('Done')
