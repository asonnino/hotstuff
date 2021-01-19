from fabric import task, Connection, ThreadingGroup as Group
from benchmark.local import LocalBench


@task
def local(ctx, nodes=4, txs=250000, size=512, rate=10000, delay=30):
    bench = LocalBench(nodes, txs, size, rate)
    bench.run(delay)
    
