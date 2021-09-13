# This script produces the plots used in the paper.

from glob import glob
from os.path import join
import os
from itertools import cycle
from re import search
from copy import deepcopy
from statistics import mean, stdev
from collections import defaultdict
from re import findall, search, split
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from itertools import cycle


# --- PARSE DATA ---


class Setup:
    def __init__(self, nodes, rate, tx_size, faults):
        self.nodes = nodes
        self.rate = rate
        self.tx_size = tx_size
        self.faults = faults
        self.max_latency = 'any'

    def __str__(self):
        return (
            f' Committee size: {self.nodes} nodes\n'
            f' Input rate: {self.rate} tx/s\n'
            f' Transaction size: {self.tx_size} B\n'
            f' Faults: {self.faults} nodes\n'
            f' Max latency: {self.max_latency} ms\n'
        )

    def __eq__(self, other):
        return isinstance(other, Setup) and str(self) == str(other)

    def __hash__(self):
        return hash(str(self))

    @classmethod
    def from_str(cls, raw):
        nodes = int(search(r'.* Committee size: (\d+)', raw).group(1))
        rate = int(search(r'.* Input rate: (\d+)', raw).group(1))
        tx_size = int(search(r'.* Transaction size: (\d+)', raw).group(1))
        faults = int(search(r'.* Faults: (\d+)', raw).group(1))
        return cls(nodes, rate, tx_size, faults)


class Result:
    def __init__(self, mean_tps, mean_latency, std_tps=0, std_latency=0):
        self.mean_tps = mean_tps
        self.mean_latency = mean_latency
        self.std_tps = std_tps
        self.std_latency = std_latency

    def __str__(self):
        return(
            f' TPS: {self.mean_tps} +/- {self.std_tps} tx/s\n'
            f' Latency: {self.mean_latency} +/- {self.std_latency} ms\n'
        )

    @classmethod
    def from_str(cls, raw):
        tps = int(search(r'.* End-to-end TPS: (\d+)', raw).group(1))
        latency = int(search(r'.* End-to-end latency: (\d+)', raw).group(1))
        return cls(tps, latency)

    @classmethod
    def aggregate(cls, results):
        if len(results) == 1:
            return results[0]

        mean_tps = round(mean([x.mean_tps for x in results]))
        mean_latency = round(mean([x.mean_latency for x in results]))
        std_tps = round(stdev([x.mean_tps for x in results]))
        std_latency = round(stdev([x.mean_latency for x in results]))
        return cls(mean_tps, mean_latency, std_tps, std_latency)


class LogAggregator:
    def __init__(self, system, files, max_latencies):
        assert isinstance(system, str)
        assert isinstance(files, list)
        assert all(isinstance(x, str) for x in files)
        assert isinstance(max_latencies, list)
        assert all(isinstance(x, int) for x in max_latencies)

        self.system = system
        self.max_latencies = max_latencies

        data = ''
        for filename in files:
            with open(filename, 'r') as f:
                data += f.read()

        records = defaultdict(list)
        for chunk in data.replace(',', '').split('SUMMARY')[1:]:
            if chunk:
                records[Setup.from_str(chunk)] += [
                    Result.from_str(chunk)
                ]

        self.records = {k: Result.aggregate(v) for k, v in records.items()}

    def print(self):
        results = [self._print_latency(), self._print_tps()]
        for graph_type, records in results:
            for setup, values in records.items():
                data = '\n'.join(
                    f' Variable value: X={x}\n{y}' for x, y in values
                )
                string = (
                    '\n'
                    '-----------------------------------------\n'
                    ' RESULTS:\n'
                    '-----------------------------------------\n'
                    f'{setup}'
                    '\n'
                    f'{data}'
                    '-----------------------------------------\n'
                )
                filename = (
                    f'{self.system}.'
                    f'{graph_type}-'
                    f'{setup.nodes}-'
                    f'{setup.rate}-'
                    f'{setup.tx_size}-'
                    f'{setup.faults}-'
                    f'{setup.max_latency}.txt'
                )
                with open(filename, 'w') as f:
                    f.write(string)

    def _print_latency(self):
        records = deepcopy(self.records)
        organized = defaultdict(list)
        for setup, result in records.items():
            rate = setup.rate
            setup.rate = 'any'
            organized[setup] += [(result.mean_tps, result, rate)]

        for setup, results in list(organized.items()):
            results.sort(key=lambda x: x[2])
            organized[setup] = [(x, y) for x, y, _ in results]

        return 'latency', organized

    def _print_tps(self):
        records = deepcopy(self.records)
        organized = defaultdict(list)
        for max_latency in self.max_latencies:
            for setup, result in records.items():
                setup = deepcopy(setup)
                if result.mean_latency <= max_latency:
                    nodes = setup.nodes
                    setup.nodes = 'x'
                    setup.rate = 'any'
                    setup.max_latency = max_latency

                    new_point = all(nodes != x[0] for x in organized[setup])
                    highest_tps = False
                    for w, r in organized[setup]:
                        if result.mean_tps > r.mean_tps and nodes == w:
                            organized[setup].remove((w, r))
                            highest_tps = True
                    if new_point or highest_tps:
                        organized[setup] += [(nodes, result)]

        [v.sort(key=lambda x: x[0]) for v in organized.values()]
        return 'tps', organized


# --- MAKE THE PLOTS ---


@ticker.FuncFormatter
def default_major_formatter(x, pos):
    if x >= 1_000:
        return f'{x/1000:.0f}k'
    else:
        return f'{x:.0f}'


def sec_major_formatter(x, pos):
    return f'{float(x)/1000:.1f}'


class PlotError(Exception):
    pass


class Ploter:
    def __init__(self, width=6.4, height=4.8):
        plt.figure(figsize=(width, height))

        self.markers = cycle(['o', 'v', 's', 'd'])
        self.styles = cycle(['solid', 'dashed', 'dotted'])
        self.colors = cycle(['tab:orange', 'tab:blue', 'tab:green'])

    def _natural_keys(self, text):
        def try_cast(text): return int(text) if text.isdigit() else text
        return [try_cast(c) for c in split('(\d+)', text)]

    def _tps(self, data):
        values = findall(r' TPS: (\d+) \+/- (\d+)', data)
        values = [(int(x), int(y)) for x, y in values]
        return list(zip(*values))

    def _latency(self, data):
        values = findall(r' Latency: (\d+) \+/- (\d+)', data)
        values = [(int(x), int(y)) for x, y in values]
        return list(zip(*values))

    def _variable(self, data):
        return [int(x) for x in findall(r'Variable value: X=(\d+)', data)]

    def _tps2bps(self, x):
        data = self.results[0]
        size = int(search(r'Transaction size: (\d+)', data).group(1))
        return x * size / 10**6

    def _bps2tps(self, x):
        data = self.results[0]
        size = int(search(r'Transaction size: (\d+)', data).group(1))
        return x * 10**6 / size

    def _plot(self, x_label, y_label, y_axis, z_axis, type, marker, color):
        self.results.sort(key=self._natural_keys, reverse=(type == 'tps'))
        for result in self.results:
            y_values, y_err = y_axis(result)
            x_values = self._variable(result)
            if len(y_values) != len(y_err) or len(y_err) != len(x_values):
                raise PlotError('Unequal number of x, y, and y_err values')

            style = next(self.styles)
            plt.errorbar(
                x_values, y_values, yerr=y_err, label=z_axis(result),
                linestyle=style, marker=marker, color=color, capsize=3, linewidth=2
            )

        plt.xlabel(x_label, fontweight='bold')
        plt.ylabel(y_label[0], fontweight='bold')
        plt.xticks(weight='bold')
        plt.yticks(weight='bold')
        ax = plt.gca()
        ax.xaxis.set_major_formatter(default_major_formatter)
        if 'latency' in type:
            ax.yaxis.set_major_formatter(sec_major_formatter)
        else:
            ax.yaxis.set_major_formatter(default_major_formatter)
        if len(y_label) > 1:
            secaxy = ax.secondary_yaxis(
                'right', functions=(self._tps2bps, self._bps2tps)
            )
            secaxy.set_ylabel(y_label[1])
            secaxy.yaxis.set_major_formatter(default_major_formatter)

    def _nodes(self, data):
        x = search(r'Committee size: (\d+)', data).group(1)
        f = search(r'Faults: (\d+)', data).group(1)
        faults = f' ({f} faulty)' if f != '0' else ''
        name = self.legend_name(self.system)
        return f'{name}, {x} nodes{faults}'

    def _max_latency(self, data):
        x = search(r'Max latency: (\d+)', data).group(1)
        f = search(r'Faults: (\d+)', data).group(1)
        faults = f' ({f} faulty)' if f != '0' else ''
        name = self.legend_name(self.system)
        return f'{name}{faults}, Max latency: {float(x)/1000:,.1f}s'

    def _input_rate(self, data):
        x = search(r'Input rate: (\d+)', data).group(1)
        f = search(r'Faults: (\d+)', data).group(1)
        faults = f' ({f} faulty)' if f != '0' else ''
        name = self.legend_name(self.system)
        return f'{name}{faults}, Input rate: {float(x)/1000:,.0f}k'

    @staticmethod
    def legend_name(system):
        if '2-chain' in system:
            return 'Round-Robin'
        elif 'leader-reputation' in system:
            return 'Carousel'
        else:
            return system.capitalize()

    def plot_latency(self, system, nodes, faults, tx_size, graph_type='latency'):
        assert isinstance(system, str)
        assert isinstance(nodes, list)
        assert all(isinstance(x, int) for x in nodes)
        assert isinstance(faults, list)
        assert all(isinstance(x, int) for x in faults)
        assert isinstance(tx_size, int)

        self.results = []
        for f in faults:
            for n in nodes:
                filename = f'{system}.{graph_type}-{n}-any-{tx_size}-{f}-any.txt'
                with open(filename, 'r') as file:
                    self.results += [file.read().replace(',', '')]

        self.system = system
        z_axis = self._nodes
        x_label = 'Throughput (tx/s)'
        y_label = ['Latency (s)']
        marker = next(self.markers)
        color = next(self.colors)
        self._plot(
            x_label, y_label, self._latency, z_axis, 'latency', marker, color
        )

    def plot_tps(self, system, faults, max_latencies, tx_size, graph_type='tps'):
        assert isinstance(system, str)
        assert isinstance(faults, list)
        assert all(isinstance(x, int) for x in faults)
        assert isinstance(max_latencies, list)
        assert all(isinstance(x, int) for x in max_latencies)
        assert isinstance(tx_size, int)

        self.results = []
        for f in faults:
            for l in max_latencies:
                filename = f'{system}.{graph_type}-x-any-{tx_size}-{f}-{l}.txt'
                with open(filename, 'r') as file:
                    self.results += [file.read().replace(',', '')]

        self.system = system
        z_axis = self._max_latency
        x_label = 'Committee size'
        y_label = ['Throughput (tx/s)', 'Throughput (MB/s)']
        marker = next(self.markers)
        color = next(self.colors)
        self._plot(x_label, y_label, self._tps, z_axis, 'tps', marker, color)

    def finalize(self, name, legend_cols, top_lim=None, legend_loc=None, legend_anchor=None):
        assert isinstance(name, str)

        plt.legend(
            loc=legend_loc, bbox_to_anchor=legend_anchor, ncol=legend_cols
        )
        plt.xlim(xmin=0)
        plt.ylim(bottom=0, top=top_lim)
        plt.grid(True)

        for x in ['pdf', 'png']:
            plt.savefig(f'{name}.{x}', bbox_inches='tight')


if __name__ == '__main__':
    max_latencies = [2_000, 5_000]  # For TPS graphs.

    # Parse the results.
    for system in ['2-chain', 'leader-reputation']:
        [os.remove(x) for x in glob(f'{system}.*.txt')]
        files = glob(join(system, 'results', '*.txt'))
        LogAggregator(system, files, max_latencies).print()

    # Plot 'Happy path' graph.
    ploter = Ploter(width=12.8)
    for system in ['2-chain', 'leader-reputation']:
        ploter.plot_latency(system, [10, 20, 50], [0], 512)
    ploter.finalize(
        # 'happy-path', legend_cols=2, top_lim=8_000, legend_anchor=(0.25, 0.79)
        'happy-path', 
        legend_cols=1, 
        top_lim=8_000, 
        legend_loc='upper left',
        legend_anchor=(0, 1)
    )

    # Plot 'Happy path TPS' graph.
    '''
    ploter = Ploter()
    for system in ['2-chain', 'leader-reputation']:
        ploter.plot_tps(system, [0], max_latencies, 512)
    ploter.finalize('happy-path-tps', legend_cols=2)
    '''

    # Plot 'Dead nodes' graph.
    ploter = Ploter(width=12.8)
    for system in ['2-chain', 'leader-reputation']:
        ploter.plot_latency(system, [10], [0, 1, 3], 512)
    ploter.finalize(
        'dead-nodes', 
        legend_cols=2, 
        top_lim=30_000, 
        legend_loc='upper center',
        legend_anchor=(0.5, 1)
    )

    # Remove aggregated log files.
    for system in ['2-chain', 'leader-reputation']:
        [os.remove(x) for x in glob(f'{system}.*.txt')]
