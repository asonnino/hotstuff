from fabric import task
from glob import glob

from benchmark.local import LocalBench
from benchmark.logs import ParseError, LogParser
from benchmark.aggregator import LogAggregator
from benchmark.utils import Print
from benchmark.plot import Ploter, PlotError
from aws.instance import InstanceManager
from aws.remote import Bench, BenchError

# NOTE: Also requires tmux: brew install tmux


@task
def local(ct):
    bench_params = {
        'nodes': 4,
        'rate': 1_000,
        'tx_size': 512,
        'duration': 20,
    }
    node_params = {
        'consensus': {
            'timeout_delay': 100,
            'sync_retry_delay': 10_000,
            'max_payload_size': 500,
            'min_block_delay': 0,
            'network_delay': 100
        },
        'mempool': {
            'queue_capacity': 10_000,
            'max_payload_size': 15_000,
            'min_block_delay': 0
        },
        'protocol': 1, # 0 HotStuff, 1 HotStuffWithAsyncFallback, 2 ChainedVABA
        'crash': 1  # crash f nodes from the beginning
    }
    try:
        ret = LocalBench(bench_params, node_params).run(debug=False).result()
        print(ret)
    except BenchError as e:
        Print.error(e)


@task
def create(ctx, nodes=4):
    try:
        InstanceManager.make().create_instances(nodes)
    except BenchError as e:
        Print.error(e)


@task
def destroy(ctx):
    try:
        InstanceManager.make().terminate_instances()
    except BenchError as e:
        Print.error(e)


@task
def start(ctx):
    try:
        InstanceManager.make().start_instances()
    except BenchError as e:
        Print.error(e)


@task
def stop(ctx):
    try:
        InstanceManager.make().stop_instances()
    except BenchError as e:
        Print.error(e)


@task
def info(ctx):
    try:
        InstanceManager.make().print_info()
    except BenchError as e:
        Print.error(e)


@task
def install(ctx):
    try:
        Bench(ctx).install()
    except BenchError as e:
        Print.error(e)


@task
def remote(ctx):
    bench_params = {
        'nodes': [4],
        'rate': [35_000],
        'tx_size': 512,
        'duration': 300,
        'runs': 4,
    }
    node_params = {
        'consensus': {
            'timeout_delay': 60_000,
            'sync_retry_delay': 500_000,
            'max_payload_size': 1_000,
            'min_block_delay': 100
        },
        'mempool': {
            'queue_capacity': 100_000_000,
            'max_payload_size': 2_000_000,
            'min_block_delay': 0
        },
        'protocol': 1, # 0 HotStuff, 1 HotStuffWithAsyncFallback, 2 ChainedVABA
        'crash': 0  # crash f nodes from the beginning
    }
    try:
        Bench(ctx).run(bench_params, node_params, debug=False)
    except BenchError as e:
        Print.error(e)


@task
def kill(ctx):
    try:
        Bench(ctx).kill()
    except BenchError as e:
        Print.error(e)


@task
def logs(ctx):
    try:
        print(LogParser.process('./logs').result())
    except ParseError as e:
        Print.error(BenchError('Failed to parse logs', e))


@task
def aggregate(ctx):
    LogAggregator().print()


@task
def plot(ctx):
    try:
        ploter = Ploter(glob('plot/*.txt'))
        #ploter.plot_tps(ploter.tx_size)
        ploter.plot_latency(ploter.nodes)
    except PlotError as e:
        Print.error(BenchError('Failed to plot performance', e))
