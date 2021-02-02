from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from paramiko import RSAKey
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from os.path import basename, splitext
from time import sleep
from math import ceil
import subprocess

from benchmark.config import Committee, Key, NodeParameters, BenchParameters, ConfigError
from benchmark.utils import BenchError, Print, PathMaker
from benchmark.commands import CommandMaker
from benchmark.logs import LogParser, ParseError
from aws.instance import InstanceManager


class FabricError(Exception):
    ''' Wrapper for Fabric exception with a meaningfull error message. '''
    def __init__(self, error):
        assert isinstance(error, GroupException)
        message = list(error.result.values())[0]
        super().__init__(message)


class Bench:
    def __init__(self, ctx):
        self.manager = InstanceManager.make()
        self.settings = self.manager.settings
        try:
            ctx.connect_kwargs.pkey = RSAKey.from_private_key_file(
                self.manager.settings.key_path
            )
            self.connect_kwargs = ctx.connect_kwargs
        except (IOError, PasswordRequiredException, SSHException) as e:
            raise BenchError('Failed to load SSH key', e)

    def install(self):
        Print.info('Installing rust and cloning the repo...')
        cmd = [
            'sudo apt update',
            'sudo apt -y upgrade',
            'sudo apt -y autoremove',

            # The following dependencies prevent the error: [error: linker `cc` not found]
            'sudo apt -y install build-essential',
            'sudo apt -y install cmake',

            # Install rust (non-interactive)
            'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y',
            'source $HOME/.cargo/env',
            'rustup default stable',

            # This is missing from the RockDB installer (needed for RockDB).
            'sudo apt install -y clang',

            # Clone the repo.
            f'git clone {self.settings.repo_url} || (cd {self.settings.repo_name} ; git pull)'
        ]
        hosts = self.manager.hosts()
        try:
            g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect_kwargs)
            g.run(' && '.join(cmd), hide=True)
            Print.heading(f'Testbed of {len(hosts)} nodes successfully initialized')
        except GroupException as e:
            raise BenchError('Failed to install repo on testbed', FabricError(e))

    def kill(self, hosts=[], delete_logs=False):
        assert isinstance(hosts, list)
        assert isinstance(delete_logs, bool)
        hosts = hosts if hosts else self.manager.hosts()
        delete_logs = 'rm -r logs ; mkdir -p logs' if delete_logs else 'true'
        cmd = [delete_logs, f'({CommandMaker.kill()} || true)']
        try:
            g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect_kwargs)
            g.run(' && '.join(cmd), hide=True)
        except GroupException as e:
            raise BenchError('Failed to kill nodes', FabricError(e))

    def _select_hosts(self, nodes):
        # TODO: Ensure there are enough hosts.
        # TODO: Select the hosts to be in different data centers,
        # and as far apart as possible (geographically).
        return self.manager.hosts()[:nodes]

    def _background_run(self, host, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'tmux new -d -s "{name}" "{command} |& tee {log_file}"'
        c = Connection(host, user='ubuntu', connect_kwargs=self.connect_kwargs)
        c.run(cmd, hide=True)

    def _update(self, hosts):
        Print.info(f'Updating {len(hosts)} nodes...')
        cmd = [
            f'(cd {self.settings.repo_name} && git fetch)',
            f'(cd {self.settings.repo_name} && git pull)',
            f'(cd {self.settings.repo_name} && git checkout {self.settings.branch})',
            'source $HOME/.cargo/env',
            f'(cd {self.settings.repo_name}/node && {CommandMaker.compile()})',
            CommandMaker.alias_binaries(f'./{self.settings.repo_name}/target/release/')
        ]
        g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect_kwargs)
        g.run(' && '.join(cmd), hide=True)

    def _config(self, hosts, node_parameters):
        Print.info('Generating configuration files...')

        # Cleanup all local configuration files.
        cmd = CommandMaker.cleanup()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
        sleep(0.5) # Removing the store may take time.

        # Recompile the latest code.
        cmd = CommandMaker.compile().split()
        subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

        # Create alias for the client and nodes binary.
        cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
        subprocess.run([cmd], shell=True)

        # Generate configuration files.
        keys = []
        key_files = [PathMaker.key_file(i) for i in range(len(hosts))]
        for filename in key_files:
            cmd = CommandMaker.generate_key(filename).split()
            subprocess.run(cmd, check=True)
            keys += [Key.from_file(filename)]

        names = [x.name for x in keys]
        consensus_addr = [f'{x}:{self.settings.consensus_port}' for x in hosts]
        mempool_addr = [f'{x}:{self.settings.mempool_port}' for x in hosts]
        front_addr = [f'{x}:{self.settings.front_port}' for x in hosts]
        committee = Committee(names, consensus_addr, mempool_addr, front_addr)
        committee.print(PathMaker.committee_file())

        node_parameters.print(PathMaker.parameters_file())

        # Cleanup all nodes.
        cmd = ' && '.join([CommandMaker.kill(), CommandMaker.cleanup()])
        g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect_kwargs)
        g.run(cmd, hide=True)

        # Upload configuration files.
        for i, host in enumerate(hosts):
            print(f' [{i+1}/{len(hosts)}] Uploading config files...', end='\r')
            c = Connection(host, user='ubuntu', connect_kwargs=self.connect_kwargs)
            c.put(PathMaker.committee_file(), '.')
            c.put(PathMaker.key_file(i), '.')
            c.put(PathMaker.parameters_file(), '.')

        return committee

    def _run_single(self, hosts, committee, bench_parameters, node_parameters, debug=False):
        # Kill any potentially unfinished run and delete logs.
        self.kill(hosts=hosts, delete_logs=True)
        subprocess.run(['rm -r logs ; mkdir -p logs'], shell=True, stderr=subprocess.DEVNULL)
        
        # Run the clients (they will wait for the nodes to be ready).
        addresses = committee.front_addresses()
        txs_share = ceil(bench_parameters.txs / bench_parameters.nodes)
        rate_share = ceil(bench_parameters.rate / bench_parameters.nodes)
        timeout = node_parameters.timeout_delay
        client_logs = [PathMaker.client_log_file(i) for i in range(len(hosts))]
        for host, addr, log_file in zip(hosts, addresses, client_logs):
            cmd = CommandMaker.run_client(
                addr,
                txs_share,
                bench_parameters.size,
                rate_share,
                timeout,
                nodes=addresses
            )
            self._background_run(host, cmd, log_file)

        # Run the nodes.
        key_files = [PathMaker.key_file(i) for i in range(len(hosts))]
        dbs = [PathMaker.db_path(i) for i in range(len(hosts))]
        node_logs = [PathMaker.node_log_file(i) for i in range(len(hosts))]
        for host, key_file, db, log_file in zip(hosts, key_files, dbs, node_logs):
            cmd = CommandMaker.run_node(
                key_file,
                PathMaker.committee_file(),
                db,
                PathMaker.parameters_file(),
                debug=debug
            )
            self._background_run(host, cmd, log_file)

        # Wait for all transactions to be processed.
        sleep(bench_parameters.duration)
        self.kill(hosts=hosts, delete_logs=False)

    def _logs(self, hosts):
        # Download log files.
        for i, host in enumerate(hosts):
            print(f' [{i+1}/{len(hosts)}] Downloading logs...', end='\r')
            c = Connection(host, user='ubuntu', connect_kwargs=self.connect_kwargs)
            c.get(PathMaker.node_log_file(i), local=PathMaker.node_log_file(i))
            c.get(PathMaker.client_log_file(i), local=PathMaker.client_log_file(i))

        # Parse logs and return the parser.
        Print.info('Parsing logs and computing performance...')
        return LogParser.process('./logs')

    def run(self, bench_parameters_dict, node_parameters_dict, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting remote benchmark')
        try:
            bench_parameters = BenchParameters(bench_parameters_dict)
            node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

        # Select which hosts to use.
        hosts = self._select_hosts(bench_parameters.nodes)

        # Update nodes.
        try:
            self._update(hosts)
        except GroupException as e:
            raise BenchError('Failed to update nodes', FabricError(e))

        # Upload all configuration files.
        try:
            committee = self._config(hosts, node_parameters)
        except (subprocess.SubprocessError, GroupException) as e:
            raise BenchError('Failed to configure nodes', FabricError(e))

        # Run the benchmark.
        runs = bench_parameters.runs
        for i in range(runs):
            Print.heading(f'[{i+1}/{runs}] Running benchmark ({bench_parameters.duration} sec)...')
            try:
                self._run_single(hosts, committee, bench_parameters, node_parameters, debug)
                parser = self._logs(hosts)
                # TODO: save the parser and handle multiple runs.
                parser.print_summary()
            except (subprocess.SubprocessError, GroupException, ParseError) as e:
                self.kill(hosts=hosts)
                Print.error(BenchError('Benchmark failed', e))
                continue