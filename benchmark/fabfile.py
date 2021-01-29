from fabric import task, Connection, ThreadingGroup as Group

from benchmark.local import LocalBench, BenchError
from benchmark.logs import ParseError
from benchmark.utils import Print
# NOTE: Also requires tmux: brew install tmux


@task
def local(ctx, nodes=4, txs=250_000, size=512, rate=150_000, duration=20, debug=False):
    try:
        LocalBench(nodes, txs, size, rate, duration).run(debug).print_summary()
    except (BenchError, ParseError) as e:
        Print.error('Failed to run benchmark', cause=str(e))
