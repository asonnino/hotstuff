from glob import glob
from os.path import join
import os

from matplotlib.pyplot import hexbin

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
    ploter = Ploter(width=12.8)
    for system in ['3-chain', '2-chain', 'ditto-sync', 'vaba']:
        ploter.plot_latency(system, [10, 20, 50], [0], 512)
    ploter.finalize('happy-path', legend_cols=4)

    # Plot 'Happy path TPS' graph.
    ploter = Ploter()
    for system in ['3-chain', '2-chain', 'ditto-sync', 'vaba']:
        ploter.plot_tps(system, [0], max_latencies, 512)
    ploter.finalize('happy-path-tps', legend_cols=2)

    # Plot 'Leader under DoS' graph.
    ploter = Ploter()
    for i, system in enumerate(['3-chain', '2-chain']):
        name = Ploter.legend_name(system)
        ploter.plot_free(
            [x + i*1_000 for x in range(0, 60_000, 10_000)], 
            [0] * 6, 
            [f'{name}, {x} nodes' for x in [10, 20, 30]]
        )
    for system in ['ditto-async', 'vaba']:
        ploter.plot_latency(system, [10, 20, 50], [0], 512)
    ploter.finalize('leader-under-dos', legend_cols=2)

    # Plot 'Dead nodes' graph.
    ploter = Ploter(width=12.8)
    for system in ['3-chain', '2-chain', 'ditto-sync', 'vaba']:
        ploter.plot_latency(system, [20], [0, 1, 3], 512)
    ploter.finalize('dead-nodes', legend_cols=4)

    # Plot 'Dead nodes and DoS' graph.
    ploter = Ploter()
    for i, system in enumerate(['3-chain', '2-chain']):
        name = Ploter.legend_name(system)
        ploter.plot_free(
            [x + i*1_000 for x in range(0, 60_000, 10_000)], 
            [0] * 6, 
            [f'{name}, {x} nodes' for x in [10, 20, 30]]
        )
    for system in ['ditto-async', 'vaba']:
        ploter.plot_latency(system, [20], [0, 1, 3], 512)
    ploter.finalize('dead-nodes-and-dos', legend_cols=2)
