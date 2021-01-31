import subprocess
from math import ceil
from os.path import basename, join, splitext
from time import sleep

from benchmark.commands import CommandMaker
from benchmark.config import Key, LocalCommittee, Parameters
from benchmark.logs import LogParser, ParseError
from benchmark.utils import Print


class LocalBenchError(Exception):
    pass


class LocalBench:
    BINARY_PATH = '../target/release/'
    NODE_CRATE_PATH = '../node'

    def __init__(self, nodes, txs, size, rate, duration):
        assert isinstance(nodes, int) and nodes > 0
        assert isinstance(txs, int) and txs >= 0
        assert isinstance(size, int) and size > 0
        assert isinstance(rate, int) and rate >= 0

        self.nodes = nodes
        self.txs = txs
        self.size = size
        self.rate = rate
        self.duration = duration

        self.base_port = 7000
        self.committee_file = '.committee.json'
        self.parameters_file = '.parameters.json'
        self.key_files = [f'.node-{i}.json' for i in range(nodes)]
        self.node_logs = [f'logs/node-{i}.log' for i in range(nodes)]
        self.dbs = [f'.db-{i}' for i in range(nodes)]
        self.client_logs = [f'logs/client-{i}.log' for i in range(nodes)]

    def _background_run(self, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'{command} 2> {log_file}'
        subprocess.run(['tmux', 'new', '-d', '-s', name, cmd], check=True)

    def _kill_nodes(self):
        cmd = CommandMaker.kill().split()
        subprocess.run(cmd, stderr=subprocess.DEVNULL)

    def run(self, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting local benchmark')

        try:
            # Kill any previous testbed and cleanup all files.
            Print.info('Setting up testbed...')
            self._kill_nodes()
            cmd = CommandMaker.cleanup()
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5) # Removing the store may take time.

            # Recompile the latest code.
            cmd = CommandMaker.compile().split()
            subprocess.run(cmd, check=True, cwd=self.NODE_CRATE_PATH)

            # Create alias for the client and nodes binary.
            cmd = CommandMaker.alias_binaries(self.BINARY_PATH)
            subprocess.run([cmd], shell=True)

            # Generate configuration files.
            keys = []
            for filename in self.key_files:
                cmd = CommandMaker.generate_key(filename).split()
                subprocess.run(cmd, check=True)
                keys += [Key.from_file(filename)]

            names = [x.name for x in keys]
            committee = LocalCommittee(names, self.base_port)
            committee.print(self.committee_file)

            parameters = Parameters.default()
            parameters.print(self.parameters_file)

            # Run the clients (they will wait for the nodes to be ready).
            addresses = committee.front_addresses()
            load = ceil(self.txs / self.nodes)
            rate = ceil(self.rate / self.nodes)
            timeout = parameters.timeout_delay
            for addr, log_file in zip(addresses, self.client_logs):
                cmd = CommandMaker.run_client(
                    addr,
                    load,
                    self.size,
                    rate,
                    timeout
                )
                self._background_run(cmd, log_file)

            # Run the nodes.
            for key_file, db, log_file in zip(self.key_files, self.dbs, self.node_logs):
                cmd = CommandMaker.run_node(
                    key_file,
                    self.committee_file,
                    db,
                    self.parameters_file,
                    debug=debug
                )
                self._background_run(cmd, log_file)

            # Wait for all transactions to be processed.
            Print.info(f'Running benchmark ({self.duration} sec)...')
            sleep(self.duration)
            self._kill_nodes()

            # Parse logs and return the parser.
            Print.info('Parsing logs...')
            return LogParser.process('./logs')

        except (subprocess.SubprocessError, ParseError) as e:
            self._kill_nodes()
            raise LocalBenchError(str(e))
