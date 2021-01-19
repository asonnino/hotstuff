from fabric import task, Connection, ThreadingGroup as Group
from benchmark.local import LocalBench
# NOTE: Also requires tmux: brew install tmux

@task
def local(ctx, nodes=4, txs=250000, size=512, rate=10000, delay=15):
    bench = LocalBench(nodes, txs, size, rate)
    bench.run(delay)
    
