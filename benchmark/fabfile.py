from fabric import task, Connection, ThreadingGroup as Group
from benchmark.local import LocalBench, BenchError
from benchmark.logs import ParseError
# NOTE: Also requires tmux: brew install tmux

@task
def local(ctx, nodes=4, txs=250000, size=512, rate=10000, delay=20):
    try:
        LocalBench(nodes, txs, size, rate).run(delay).print_summary()
    except (BenchError, ParseError) as e:
        print(e)
    
