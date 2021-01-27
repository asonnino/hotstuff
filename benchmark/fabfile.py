from fabric import task, Connection, ThreadingGroup as Group
from benchmark.local import LocalBench, BenchError
from benchmark.logs import ParseError
from benchmark.utils import Print
# NOTE: Also requires tmux: brew install tmux

@task
def local(ctx, nodes=4, txs=250000, size=512, rate=0, delay=20, debug=False):
    try:
        LocalBench(nodes, txs, size, rate).run(delay, debug).print_summary()
    except (BenchError, ParseError) as e:
        Print.error('Failed to run benchmark', cause=str(e))
    
