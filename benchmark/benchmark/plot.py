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
        avg = [int(x) for x in findall(r'Average TPS: (\d+)', data)]
        std = [int(x) for x in findall(r'Std TPS: (\d+)', data)]
        return avg, std

    def _latency(self, data):
        avg = [int(x) for x in findall(r'Average latency: (\d+)', data)]
        std = [int(x) for x in findall(r'Std latency: (\d+)', data)]
        return avg, std

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

    def tx_size(self, data):
        return search(r'Transaction size: .*', data).group(0)

    def txs_rate(self, data):
        val = search(r'Transaction rate: (\d+)', data).group(1)
        return f'Transaction rate: {int(val):,} tx/s'

    def _plot(self, x_label, y_label, y_axis, z_axis, filename):
        plt.figure()
        for result in self.results:
            y_values, y_err = y_axis(result)
            x_values = self._variable(result)
            assert len(y_values) == len(y_err) and len(y_err) == len(x_values)

            plt.errorbar(
                x_values, y_values, yerr=y_err,  # uplims=True, lolims=True,
                marker='o', label=z_axis(result), linestyle='dotted'
            )

        plt.ylim(bottom=0)
        plt.xlabel(x_label)
        plt.ylabel(y_label[0])
        plt.legend(loc='lower left')
        ax = plt.gca()
        #ax.ticklabel_format(useOffset=False, style='plain')
        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
        ax.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))
        if len(y_label) > 1:
            secaxy = ax.secondary_yaxis(
                'right', functions=(self._tps2bps, self._bps2tps)
            )
            secaxy.set_ylabel(y_label[1])
            secaxy.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))

        plt.savefig(f'{filename}.pdf', bbox_inches='tight')
        plt.savefig(f'{filename}.png', bbox_inches='tight')

    def plot_tps(self, x_label, z_axis):
        assert isinstance(x_label, str)
        assert hasattr(z_axis, '__call__')
        y_label = ['Throughput (tx/s)', 'Throughput (MB/s)']
        self._plot(x_label, y_label, self._tps, z_axis, 'tps')

    def plot_latency(self, x_label, z_axis):
        assert isinstance(x_label, str)
        assert hasattr(z_axis, '__call__')
        y_label = ['Consensus latency (ms)']
        self._plot(x_label, y_label, self._latency, z_axis, 'latency')
