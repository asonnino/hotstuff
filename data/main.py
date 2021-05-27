from glob import glob
from os.path import join
import os

from parse import LogAggregator
from plot import Ploter


if __name__ == '__main__':
    max_latencies = [2_000, 5_000]  # For tps graphs.

    # Parse the results.
    for system in ['2-chain', '3-chain', 'ditto-async', 'ditto-sync', 'vaba']:
        [os.remove(x) for x in glob(f'{system}.*.txt')]

        files = glob(join(system, 'results', '*.txt'))
        LogAggregator(system, files, max_latencies).print()

    # Plot happy path.
    ploter = Ploter()
    for system in ['2-chain', '3-chain', 'ditto-sync', 'vaba']:
        ploter.plot_latency(system, [10, 20, 50], [0], 512)
    ploter.finalize('happy-path')
