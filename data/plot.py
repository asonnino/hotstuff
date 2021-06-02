from re import findall, search, split
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from itertools import cycle


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
    MARKERS = cycle(['o', 'v', 's', 'd'])
    STYLES = cycle(['solid', 'dashed', 'dotted'])
    COLORS = cycle(['tab:green', 'tab:blue', 'tab:orange', 'tab:red'])

    def __init__(self, width=6.4, height=4.8):
        plt.figure(figsize=(width, height))

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

            style = next(self.STYLES)
            plt.errorbar(
                x_values, y_values, yerr=y_err, label=z_axis(result),
                linestyle=style, marker=marker, color=color, capsize=3
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
        if 'ditto' in system:
            return 'Ditto'
        elif '3-chain' in system:
            return 'HotStuff'
        elif '2-chain' in system:
            return 'Jolteon'
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
        marker = next(self.MARKERS)
        color = next(self.COLORS)
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
        marker = next(self.MARKERS)
        color = next(self.COLORS)
        self._plot(x_label, y_label, self._tps, z_axis, 'tps', marker, color)

    def plot_commit_lantecy(self, system, faults, rates, tx_size, graph_type='commit_latency'):
        assert isinstance(system, str)
        assert isinstance(faults, list)
        assert all(isinstance(x, int) for x in faults)
        assert isinstance(rates, list)
        assert all(isinstance(x, int) for x in rates)
        assert isinstance(tx_size, int)

        self.results = []
        for f in faults:
            for r in rates:
                filename = f'{system}.{graph_type}-x-{r}-{tx_size}-{f}-any.txt'
                with open(filename, 'r') as file:
                    self.results += [file.read().replace(',', '')]

        self.system = system
        z_axis = self._input_rate
        x_label = 'Committee size'
        y_label = ['Latency (s)']
        marker = next(self.MARKERS)
        color = next(self.COLORS)
        self.STYLES = cycle(['solid', 'dashed'])
        self._plot(x_label, y_label, self._latency, z_axis, 'commit_latency', marker, color)

    def plot_free(self, x_values, y_values, labels):
        assert isinstance(x_values, list)
        assert all(isinstance(x, int) for x in x_values)
        assert isinstance(y_values, list)
        assert all(isinstance(x, int) for x in y_values)
        assert len(x_values) == len(y_values)
        assert isinstance(labels, list)
        assert all(isinstance(x, str) for x in labels)

        marker = next(self.MARKERS)
        color = next(self.COLORS)
        for label in labels:
            style = next(self.STYLES)
            plt.plot(
                x_values, y_values, label=label,
                marker=marker, color=color, linestyle=style
            )

    def finalize(self, name, legend_cols, top_lim=None):
        assert isinstance(name, str)

        plt.legend(
            loc='lower center', bbox_to_anchor=(0.5, 1), ncol=legend_cols
        )
        plt.xlim(xmin=0)
        plt.ylim(bottom=0, top=top_lim)
        plt.grid(True)

        for x in ['pdf', 'png']:
            plt.savefig(f'{name}.{x}', bbox_inches='tight')
