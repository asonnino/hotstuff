from glob import glob
from os.path import join
import os

from parse import LogAggregator
from plot import Ploter


if __name__ == '__main__':
    max_latencies = [2_000, 5_000]  # For TPS graphs.

    # Parse the results.
    for system in ['3-chain', '2-chain', 'ditto-async', 'ditto-sync', 'vaba']:
        [os.remove(x) for x in glob(f'{system}.*.txt')]

        files = glob(join(system, 'results', '*.txt'))
        LogAggregator(system, files, max_latencies).print()

    # Plot 'Happy path' graph.
    ploter = Ploter()
    for system in ['3-chain', '2-chain', 'ditto-sync', 'vaba']:
        ploter.plot_latency(system, [10, 20, 50], [0], 512)
    ploter.finalize('happy-path')

    # Plot 'Happy path TPS' graph.
    ploter = Ploter()
    for system in ['3-chain', '2-chain', 'ditto-sync', 'vaba']:
        ploter.plot_tps(system, [0], max_latencies, 512)
    ploter.finalize('happy-path-tps')

    # Plot 'Leader under DoS' graph.
    ploter = Ploter()
    for system in ['3-chain', '2-chain']:
        ploter.plot_free(
            [10_000, 20_000, 30_000, 40_000, 50_000], [0, 0, 0, 0, 0], 
            [f'{system}, {x} nodes' for x in [10, 20, 30]]
        )
    for system in ['ditto-async', 'vaba']:
        ploter.plot_latency(system, [10, 20, 50], [0], 512)
    ploter.finalize('leader-under-dos')

    # Plot 'Dead nodes' graph.
    ploter = Ploter()
    for system in ['3-chain', '2-chain', 'ditto-sync', 'vaba']:
        ploter.plot_latency(system, [20], [0, 1, 3], 512)
    ploter.finalize('dead-nodes')
