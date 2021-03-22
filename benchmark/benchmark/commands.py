from os.path import join

from benchmark.utils import PathMaker


class CommandMaker:

    @staticmethod
    def cleanup():
        return (
            f'rm -r .db-* ; rm .*.json ; mkdir -p {PathMaker.results_path()}'
        )

    @staticmethod
    def clean_logs():
        return f'rm -r {PathMaker.logs_path()} ; mkdir -p {PathMaker.logs_path()}'

    @staticmethod
    def compile():
        return 'cargo build --quiet --release --features benchmark'

    @staticmethod
    def generate_key(filename):
        assert isinstance(filename, str)
        return f'./node keys --filename {filename}'

    @staticmethod
    def run_node(keys, threshold_keys, committee, store, parameters, debug=False):
        assert isinstance(keys, str)
        assert isinstance(threshold_keys, str)
        assert isinstance(committee, str)
        assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        return (f'./node {v} run --keys {keys} --threshold_keys {threshold_keys} --committee {committee} '
                f'--store {store} --parameters {parameters}')

    @staticmethod
    def run_client(address, size, rate, timeout, nodes=[]):
        assert isinstance(address, str)
        assert isinstance(size, int) and size > 0
        assert isinstance(rate, int) and rate >= 0
        assert isinstance(nodes, list)
        assert all(isinstance(x, str) for x in nodes)
        nodes = f'--nodes {" ".join(nodes)}' if nodes else ''
        return (f'./client {address} --size {size} '
                f'--rate {rate} --timeout {timeout} {nodes}')

    @staticmethod
    def kill():
        return 'tmux kill-server'

    @staticmethod
    def alias_binaries(origin):
        assert isinstance(origin, str)
        node, client = join(origin, 'node'), join(origin, 'client')
        return f'rm node ; rm client ; ln -s {node} . ; ln -s {client} .'
