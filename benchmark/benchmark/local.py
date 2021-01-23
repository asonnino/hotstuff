import subprocess
from time import sleep
from math import ceil
from os.path import join, splitext, basename

from benchmark.commands import CommandMaker
from benchmark.committee import LocalCommittee, Key
from benchmark.logs import LogParser


class BenchError(Exception):
    pass

class LocalBench:
    BINARY_PATH = '../target/release/'
    NODE_CRATE_PATH = '../node'

    def __init__(self, nodes, txs, size, rate):
        assert isinstance(nodes, int) and nodes > 0
        assert isinstance(txs, int) and txs >= 0
        assert isinstance(size, int) and size > 0
        assert isinstance(rate, int) and rate >= 0

        self.nodes = nodes
        self.txs = txs
        self.size = size
        self.rate = rate

        self.base_port = 7000
        self.committee_file = '.committee.json'
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

    def run(self, delay):
        assert isinstance(delay, int) and delay > 0
        print(f'Running local benchmark...')

        try:
            # Kill any previous testbed and cleanup all files.
            self._kill_nodes()
            cmd = CommandMaker.cleanup()
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5)

            # Recompile the latest code.
            cmd = CommandMaker.compile().split()
            subprocess.run(
                cmd, check=True, cwd=self.NODE_CRATE_PATH, stderr=subprocess.DEVNULL
            )

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

            # Run all nodes.
            for key_file, db, log_file in zip(self.key_files, self.dbs, self.node_logs):
                cmd = CommandMaker.run_node(key_file, self.committee_file, db)
                self._background_run(cmd, log_file)

            # Wait a bit for the nodes to start and then run all clients.
            sleep(0.5)
            addresses = committee.front_addresses()
            load = ceil(self.txs / self.nodes)
            for addr, log_file in zip(addresses, self.client_logs):
                cmd = CommandMaker.run_client(addr, load, self.size, self.rate)
                self._background_run(cmd, log_file)

            # Wait for all transactions to be processed.
            sleep(delay)
            self._kill_nodes()

            # Parse logs and return the parser.
            return LogParser.process('./logs')

        except Exception as e:
            # TODO: Find out exactly which errors may be raised.
            self._kill_nodes()
            import traceback
            print(traceback.format_exc())
            raise BenchError(f'Failled to run local benchmark: {e}')