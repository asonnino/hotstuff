from fabric import task

from benchmark.local import LocalBench, LocalBenchError
from benchmark.logs import ParseError
from benchmark.utils import Print
from aws.settings import SettingsError
from aws.instance import InstanceManager, AWSError
from aws.remote import Bench, BenchError
# NOTE: Also requires tmux: brew install tmux


@task
def local(ctx, nodes=4, txs=250_000, size=512, rate=100_000, duration=20, debug=False):
    try:
        LocalBench(nodes, txs, size, rate, duration).run(debug).print_summary()
    except (LocalBenchError, ParseError) as e:
        Print.error('Failed to run benchmark', cause=e)


@task
def setup(ctx, nodes=4):
    try:        
        #InstanceManager.make().create_instances(nodes)
        # TODO: Wait for instances to set up.
        Bench(ctx).install()
    except (SettingsError, AWSError, BenchError) as e:
        Print.error('Failed to create testbed', cause=e)


@task
def destroy(ctx):
    try:
        InstanceManager.make().terminate_instances()
    except (SettingsError, AWSError) as e:
        Print.error('Failed to destroy testbed', cause=e)


@task
def start(ctx):
    try:
        InstanceManager.make().start_instances()
    except (SettingsError, AWSError) as e:
        Print.error('Failed to start instances', cause=e)


@task
def stop(ctx):
    try:
        InstanceManager.make().stop_instances()
    except (SettingsError, AWSError) as e:
        Print.error('Failed to stop instances', cause=e)


@task
def info(ctx):
    try:
        InstanceManager.make().print_info()
    except (SettingsError, AWSError) as e:
        Print.error('Failed to gather machines information', cause=e)


@task
def bench(ctx):
    # TODO
    #Bench(ctx).run()
