from fabric import task
from time import sleep
from glob import glob

from benchmark.local import LocalBench
from benchmark.logs import ParseError, LogAggregator
from benchmark.utils import Print
from benchmark.plot import Ploter, PlotError
from aws.settings import SettingsError
from aws.instance import InstanceManager, AWSError
from aws.remote import Bench, BenchError

# NOTE: Also requires tmux: brew install tmux


@task
def local(ct):
    bench_params = {
        'nodes': 4,
        'txs': 250_000,
        'size': 512,
        'rate': 150_000,
        'duration': 20,
    }
    node_params = {
        'consensus': {
            'timeout_delay': 5000,
            'sync_retry_delay': 10_000
        },
        'mempool': {
            'queue_capacity': 10_000,
            'max_payload_size': 100_000
        }
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
        'txs': 1_000_000,
        'size': 512,
        'rate': 10_000,
        'duration': 1000,
        'runs': 2,
    }
    node_params = {
        'consensus': {
            'timeout_delay': 5000,
            'sync_retry_delay': 10_000
        },
        'mempool': {
            'queue_capacity': 100_000_000,
            'max_payload_size': 2_000_000
        }
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
def aggregate(ctx):
    files = glob('benchmark.*.txt')
    try:
        LogAggregator(files).print('benchmark.txt')
    except ParseError as e:
        Print.error(BenchError('Failed to aggregate logs', e))


@task
def plot(ctx):
    files = glob('results/plot/*.txt')
    try:
        ploter = Ploter(files)
        ploter.plot_tps('Committee size', ploter.txs)
        ploter.plot_latency('Committee size', ploter.txs)
    except PlotError as e:
        Print.error(BenchError('Failed to plot performance', e))
    
