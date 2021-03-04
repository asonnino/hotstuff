from re import findall, search
from glob import glob
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator, StrMethodFormatter
from os.path import join
from statistics import mean
import sys


class PlotError(Exception):
    pass


class Ploter:
    def __init__(self, filenames):
        ok = isinstance(filenames, list) and filenames \
            and all(isinstance(x, str) for x in filenames)
        if not ok:
            raise PlotError('Invalid input arguments')

        self.results = []
        try:
            for filename in filenames:
                with open(filename, 'r') as f:
                    self.results += [f.read().replace(',', '')]
        except OSError as e:
            raise PlotError(f'Failed to load log files: {e}')

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

    def nodes(self, data):
        x = search(r'Committee size: (\d+)', data).group(1)
        return f'{x} nodes'

    def tx_size(self, data):
        return search(r'Transaction size: .*', data).group(0)

    def _plot(self, x_label, y_label, y_axis, z_axis, filename):
        plt.figure()
        for result in self.results:
            y_values, y_err = y_axis(result)
            x_values = self._variable(result)
            if len(y_values) != len(y_err) or len(y_err) != len(x_values):
                raise PlotError('Unequal number of x, y, and y_err values')

            plt.errorbar(
                x_values, y_values, yerr=y_err,  # uplims=True, lolims=True,
                marker='o', label=z_axis(result), linestyle='dotted'
            )

        plt.ylim(bottom=0)
        plt.xlabel(x_label)
        plt.ylabel(y_label[0])
        plt.legend(loc='upper right')
        ax = plt.gca()
        #ax.ticklabel_format(useOffset=False, style='plain')
        #ax.xaxis.set_major_locator(MaxNLocator(integer=True))
        ax.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))
        if len(y_label) > 1:
            secaxy = ax.secondary_yaxis(
                'right', functions=(self._tps2bps, self._bps2tps)
            )
            secaxy.set_ylabel(y_label[1])
            secaxy.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))

        plt.savefig(f'{filename}.pdf', bbox_inches='tight')
        plt.savefig(f'{filename}.png', bbox_inches='tight')

    def plot_tps(self, z_axis):
        assert hasattr(z_axis, '__call__')
        x_label = 'Committee size'
        y_label = ['Throughput (tx/s)', 'Throughput (MB/s)']
        self._plot(x_label, y_label, self._tps, z_axis, 'tps')

    def plot_latency(self, z_axis):
        assert hasattr(z_axis, '__call__')
        x_label = 'Throughput (tx/s)'
        y_label = ['Latency (ms)']
        self._plot(x_label, y_label, self._latency, z_axis, 'latency')
