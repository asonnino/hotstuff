from os.path import join


class CommandMaker:

    @staticmethod
    def cleanup():
        return ('rm -r .db-* ; rm .*.json ; rm -r logs ; '
                'rm node ; rm client ; '
                'mkdir -p logs'
                )

    @staticmethod
    def compile():
        return 'cargo build --features benchmark --release'

    @staticmethod
    def generate_key(filename):
        assert isinstance(filename, str)
        return f'./node keys --filename {filename}'

    @staticmethod
    def run_node(keys, committee, store, parameters=None):
        assert isinstance(keys, str)
        assert isinstance(committee, str)
        assert isinstance(parameters, str) or parameters is None
        params = '' if parameters is None else f'--parameters {parameters}'
        return f'./node -vv run --keys {keys} --committee {committee} --store {store} {params}'

    @staticmethod
    def run_client(address, txs, size, rate):
        assert isinstance(address, str)
        assert isinstance(txs, int)
        assert isinstance(size, int) and size > 0
        assert isinstance(rate, int) and rate >= 0
        return f'./client {address} --transactions {txs} --size {size} --rate {rate}'

    @staticmethod
    def kill():
        return 'tmux kill-server'

    @staticmethod
    def alias_binaries(origin):
        assert isinstance(origin, str)
        node, client = join(origin, 'node'), join(origin, 'client')
        return f'ln -s {node} . ; ln -s {client} .'
