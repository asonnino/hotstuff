from re import findall, search, split
import matplotlib.pyplot as plt
from matplotlib.ticker import StrMethodFormatter
from itertools import cycle


class PlotError(Exception):
    pass


class Ploter:
    def __init__(self):
        plt.figure()

    def _natural_keys(self, text):
        def try_cast(text): return int(text) if text.isdigit() else text
        return [try_cast(c) for c in split('(\d+)', text)]

    def _tps(self, data):
        values = findall(r' TPS: (\d+) \+/- (\d+)', data)
        values = [(int(x), int(y)) for x, y in values]
        return list(zip(*values))

    def _latency(self, data, scale=1):
        values = findall(r' Latency: (\d+) \+/- (\d+)', data)
        values = [(float(x)/scale, float(y)/scale) for x, y in values]
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

    def _plot(self, results, x_label, y_label, y_axis, z_axis, type):
        markers = cycle(['o', 'v', 's', 'p', 'D', 'P'])
        results.sort(key=self._natural_keys, reverse=(type == 'tps'))
        for result in results:
            y_values, y_err = y_axis(result)
            x_values = self._variable(result)
            if len(y_values) != len(y_err) or len(y_err) != len(x_values):
                raise PlotError('Unequal number of x, y, and y_err values')

            plt.errorbar(
                x_values, y_values, yerr=y_err, label=z_axis(result),
                linestyle='dotted', marker=next(markers), capsize=3
            )

        plt.legend(loc='lower center', bbox_to_anchor=(0.5, 1), ncol=2)
        plt.xlim(xmin=0)
        plt.ylim(bottom=0)
        plt.xlabel(x_label)
        plt.ylabel(y_label[0])
        plt.grid()
        ax = plt.gca()
        ax.xaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))
        ax.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))
        if len(y_label) > 1:
            secaxy = ax.secondary_yaxis(
                'right', functions=(self._tps2bps, self._bps2tps)
            )
            secaxy.set_ylabel(y_label[1])
            secaxy.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))

    def _nodes(self, data):
        x = search(r'Committee size: (\d+)', data).group(1)
        f = search(r'Faults: (\d+)', data).group(1)
        faults = f'({f} faulty)' if f != '0' else ''
        return f'{self.system}, {x} nodes {faults}'

    def _max_latency(self, data):
        x = search(r'Max latency: (\d+)', data).group(1)
        f = search(r'Faults: (\d+)', data).group(1)
        faults = f'({f} faulty)' if f != '0' else ''
        return f'{self.system} {faults}, Max latency: {float(x) / 1000:,.1f}s'

    def plot_latency(self, system, nodes, faults, tx_size):
        assert isinstance(system, str)
        assert isinstance(nodes, list)
        assert all(isinstance(x, int) for x in nodes)
        assert isinstance(faults, list)
        assert all(isinstance(x, int) for x in faults)
        assert isinstance(tx_size, int)

        results = []
        for f in faults:
            for n in nodes:
                filename = f'{system}.latency-{n}-any-{tx_size}-{f}-any.txt'
                with open(filename, 'r') as file:
                    results += [file.read().replace(',', '')]

        self.system = system
        z_axis = self._nodes
        x_label = 'Throughput (tx/s)'
        y_label = ['Latency (ms)']
        self._plot(results, x_label, y_label, self._latency, z_axis, 'latency')

    def plot_tps(self, system, faults, max_latencies, tx_size):
        assert isinstance(system, str)
        assert isinstance(faults, list)
        assert all(isinstance(x, int) for x in faults)
        assert isinstance(max_latencies, list)
        assert all(isinstance(x, int) for x in max_latencies)
        assert isinstance(tx_size, int)

        results = []
        for f in faults:
            for l in max_latencies:
                filename = f'{system}.tps-x-any-{tx_size}-{f}-{l}.txt'
                with open(filename, 'r') as file:
                    results += [file.read().replace(',', '')]

        self.system = system
        z_axis = self._max_latency
        x_label = 'Committee size'
        y_label = ['Throughput (tx/s)', 'Throughput (MB/s)']
        self._plot(results, x_label, y_label, self._tps, z_axis, 'tps')

    def finalize(self, name):
        assert isinstance(name, str)

        for x in ['pdf', 'png']:
            plt.savefig(f'{name}.{x}', bbox_inches='tight')
