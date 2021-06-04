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
        LogAggregator(system, files, max_latencies, end_to_end=False).print()

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

    # Plot 'Happy path commit latency' graph.
    ploter = Ploter()
    for system in ['3-chain', '2-chain']:
        ploter.plot_commit_lantecy(
            system, [0], [20000], 512, graph_type='commit_latency'
        )
    ploter.finalize('happy-path-commit', legend_cols=2, top_lim=1_500)

    # Plot 'Leader under DoS' graph.
    ploter = Ploter()
    for i, system in enumerate(['3-chain', '2-chain']):
        name = Ploter.legend_name(system)
        ploter.plot_free(
            [i*500], 
            [0], 
            [f'{name}, {x} nodes' for x in [10, 20, 50]]
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
            [i*500], 
            [0], 
            [
                f'{name}, 20 nodes', 
                f'{name}, 20 nodes (1 faulty)', 
                f'{name}, 20 nodes (3 faulty)'
            ]
        )
    for system in ['ditto-async', 'vaba']:
        ploter.plot_latency(system, [20], [0, 1, 3], 512)
    ploter.finalize('dead-nodes-and-dos', legend_cols=2)
