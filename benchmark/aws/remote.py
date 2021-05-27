from os import error
from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from paramiko import RSAKey
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from os.path import basename, splitext
from time import sleep
from math import ceil
from os.path import join
import subprocess

from benchmark.config import Committee, Key, TSSKey, NodeParameters, BenchParameters, ConfigError
from benchmark.utils import BenchError, Print, PathMaker, progress_bar
from benchmark.commands import CommandMaker
from benchmark.logs import LogParser, ParseError
from aws.instance import InstanceManager


class FabricError(Exception):
    ''' Wrapper for Fabric exception with a meaningfull error message. '''

    def __init__(self, error):
        assert isinstance(error, GroupException)
        message = list(error.result.values())[-1]
        super().__init__(message)


class ExecutionError(Exception):
    pass


class Bench:
    def __init__(self, ctx):
        self.manager = InstanceManager.make()
        self.settings = self.manager.settings
        try:
            ctx.connect_kwargs.pkey = RSAKey.from_private_key_file(
                self.manager.settings.key_path
            )
            self.connect = ctx.connect_kwargs
        except (IOError, PasswordRequiredException, SSHException) as e:
            raise BenchError('Failed to load SSH key', e)

    def _check_stderr(self, output):
        if isinstance(output, dict):
            for x in output.values():
                if x.stderr:
                    raise ExecutionError(x.stderr)
        else:
            if output.stderr:
                raise ExecutionError(output.stderr)

    def install(self):
        Print.info('Installing rust and cloning the repo...')
        cmd = [
            'sudo apt-get update',
            'sudo apt-get -y upgrade',
            'sudo apt-get -y autoremove',

            # The following dependencies prevent the error: [error: linker `cc` not found].
            'sudo apt-get -y install build-essential',
            'sudo apt-get -y install cmake',

            # Install rust (non-interactive).
            'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y',
            'source $HOME/.cargo/env',
            'rustup default stable',

            # This is missing from the Rocksdb installer (needed for Rocksdb).
            'sudo apt-get install -y clang',

            # Clone the repo.
            f'(git clone {self.settings.repo_url} || (cd {self.settings.repo_name} ; git pull))'
        ]
        hosts = self.manager.hosts(flat=True)
        try:
            g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect)
            g.run(' && '.join(cmd), hide=True)
            Print.heading(f'Initialized testbed of {len(hosts)} nodes')
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to install repo on testbed', e)

    def kill(self, hosts=[], delete_logs=False):
        assert isinstance(hosts, list)
        assert isinstance(delete_logs, bool)
        hosts = hosts if hosts else self.manager.hosts(flat=True)
        delete_logs = CommandMaker.clean_logs() if delete_logs else 'true'
        cmd = [delete_logs, f'({CommandMaker.kill()} || true)']
        try:
            g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect)
            g.run(' && '.join(cmd), hide=True)
        except GroupException as e:
            raise BenchError('Failed to kill nodes', FabricError(e))

    def _select_hosts(self, bench_parameters):
        nodes = max(bench_parameters.nodes)

        # Ensure there are enough hosts.
        hosts = self.manager.hosts()
        if sum(len(x) for x in hosts.values()) < nodes:
            return []

        # Select the hosts in different data centers.
        ordered = zip(*hosts.values())
        ordered = [x for y in ordered for x in y]
        return ordered[:nodes]

    def _background_run(self, host, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'tmux new -d -s "{name}" "{command} |& tee {log_file}"'
        c = Connection(host, user='ubuntu', connect_kwargs=self.connect)
        output = c.run(cmd, hide=True)
        self._check_stderr(output)

    def _update(self, hosts):
        Print.info(
            f'Updating {len(hosts)} nodes (branch "{self.settings.branch}")...'
        )
        cmd = [
            f'(cd {self.settings.repo_name} && git fetch -f)',
            f'(cd {self.settings.repo_name} && git checkout -f {self.settings.branch})',
            f'(cd {self.settings.repo_name} && git pull -f)',
            'source $HOME/.cargo/env',
            f'(cd {self.settings.repo_name}/node && {CommandMaker.compile()})',
            CommandMaker.alias_binaries(
                f'./{self.settings.repo_name}/target/release/'
            )
        ]
        g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect)
        g.run(' && '.join(cmd), hide=True)

    def _config(self, hosts, node_parameters):
        Print.info('Generating configuration files...')

        # Cleanup all local configuration files.
        cmd = CommandMaker.cleanup()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

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

        # Generate threshold signature files.
        nodes = len(hosts)
        cmd = './node threshold_keys'
        for i in range(nodes):
            cmd += ' --filename ' + PathMaker.threshold_key_file(i)
        cmd = cmd.split()
        subprocess.run(cmd, capture_output=True, check=True)

        names = [x.name for x in keys]
        consensus_addr = [f'{x}:{self.settings.consensus_port}' for x in hosts]
        front_addr = [f'{x}:{self.settings.front_port}' for x in hosts]
        tss_keys = []
        for i in range(nodes):
            tss_keys += [TSSKey.from_file(PathMaker.threshold_key_file(i))]
        ids = [x.id for x in tss_keys]
        mempool_addr = [f'{x}:{self.settings.mempool_port}' for x in hosts]
        committee = Committee(names, ids, consensus_addr, front_addr, mempool_addr)
        committee.print(PathMaker.committee_file())

        node_parameters.print(PathMaker.parameters_file())

        # Cleanup all nodes.
        cmd = f'{CommandMaker.cleanup()} || true'
        g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect)
        g.run(cmd, hide=True)

        # Upload configuration files.
        progress = progress_bar(hosts, prefix='Uploading config files:')
        for i, host in enumerate(progress):
            c = Connection(host, user='ubuntu', connect_kwargs=self.connect)
            c.put(PathMaker.committee_file(), '.')
            c.put(PathMaker.key_file(i), '.')
            c.put(PathMaker.threshold_key_file(i), '.')
            c.put(PathMaker.parameters_file(), '.')

        return committee

    def _run_single(self, hosts, rate, bench_parameters, node_parameters, debug=False):
        Print.info('Booting testbed...')

        # Kill any potentially unfinished run and delete logs.
        self.kill(hosts=hosts, delete_logs=True)

        Print.info('Killed previous instances')
        sleep(10)

        # Run the clients (they will wait for the nodes to be ready).
        # Filter all faulty nodes from the client addresses (or they will wait
        # for the faulty nodes to be online).
        committee = Committee.load(PathMaker.committee_file())
        addresses = [f'{x}:{self.settings.front_port}' for x in hosts]
        rate_share = ceil(rate / committee.size())  # Take faults into account.
        timeout = node_parameters.timeout_delay
        client_logs = [PathMaker.client_log_file(i) for i in range(len(hosts))]
        for host, addr, log_file in zip(hosts, addresses, client_logs):
            cmd = CommandMaker.run_client(
                addr,
                bench_parameters.tx_size,
                rate_share,
                timeout,
                nodes=addresses
            )
            self._background_run(host, cmd, log_file)

        Print.info('Clients boosted...')
        sleep(10)

        # Run the nodes.
        key_files = [PathMaker.key_file(i) for i in range(len(hosts))]
        dbs = [PathMaker.db_path(i) for i in range(len(hosts))]
        node_logs = [PathMaker.node_log_file(i) for i in range(len(hosts))]
        threshold_key_files = [PathMaker.threshold_key_file(i) for i in range(len(hosts))]
        for host, key_file, threshold_key_file, db, log_file in zip(hosts, key_files, threshold_key_files, dbs, node_logs):
            cmd = CommandMaker.run_node(
                key_file,
                threshold_key_file,
                PathMaker.committee_file(),
                db,
                PathMaker.parameters_file(),
                debug=debug
            )
            self._background_run(host, cmd, log_file)

        # Wait for the nodes to synchronize
        Print.info('Waiting for the nodes to synchronize...')
        sleep(node_parameters.timeout_delay / 1000)

        # Wait for all transactions to be processed.
        duration = bench_parameters.duration
        for _ in progress_bar(range(100), prefix=f'Running benchmark ({duration} sec):'):
            sleep(ceil(duration / 100))
        self.kill(hosts=hosts, delete_logs=False)

    def _logs(self, hosts, faults, protocol, ddos):
        # Delete local logs (if any).
        cmd = CommandMaker.clean_logs()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Download log files.
        progress = progress_bar(hosts, prefix='Downloading logs:')
        for i, host in enumerate(progress):
            c = Connection(host, user='ubuntu', connect_kwargs=self.connect)
            c.get(PathMaker.node_log_file(i), local=PathMaker.node_log_file(i))
            c.get(
                PathMaker.client_log_file(i), local=PathMaker.client_log_file(i)
            )

        # Parse logs and return the parser.
        Print.info('Parsing logs and computing performance...')
        return LogParser.process(PathMaker.logs_path(), faults=faults, protocol=protocol, ddos=ddos)

    def run(self, bench_parameters_dict, node_parameters_dict, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting remote benchmark')
        try:
            bench_parameters = BenchParameters(bench_parameters_dict)
            node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

        # Select which hosts to use.
        selected_hosts = self._select_hosts(bench_parameters)
        if not selected_hosts:
            Print.warn('There are not enough instances available')
            return

        # Update nodes.
        try:
            self._update(selected_hosts)
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to update nodes', e)

        if node_parameters.protocol == 0:
            Print.info('Running HotStuff')
        elif node_parameters.protocol == 1:
            Print.info('Running AsyncHotStuff')
        elif node_parameters.protocol == 2:
            Print.info('Running TwoChainVABA')
        else:
            Print.info('Wrong protocol type!')
            return

        Print.info(f'{bench_parameters.faults} faults')
        Print.info(f'Timeout {node_parameters.timeout_delay} ms, Network delay {node_parameters.network_delay} ms')
        Print.info(f'DDOS attack {node_parameters.ddos}')

        hosts = selected_hosts[:bench_parameters.nodes[0]]
        # Upload all configuration files.
        try:
            self._config(hosts, node_parameters)
        except (subprocess.SubprocessError, GroupException) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            Print.error(BenchError('Failed to configure nodes', e))
        
        # Run benchmarks.
        for n in bench_parameters.nodes:
            for r in bench_parameters.rate:
                Print.heading(f'\nRunning {n} nodes (input rate: {r:,} tx/s)')
                hosts = selected_hosts[:n]

                # # Upload all configuration files.
                # try:
                #     self._config(hosts, node_parameters)
                # except (subprocess.SubprocessError, GroupException) as e:
                #     e = FabricError(e) if isinstance(e, GroupException) else e
                #     Print.error(BenchError('Failed to configure nodes', e))
                #     continue

                # Do not boot faulty nodes.
                faults = bench_parameters.faults
                hosts = hosts[:n-faults]

                protocol = node_parameters.protocol
                ddos = node_parameters.ddos

                # Run the benchmark.
                for i in range(bench_parameters.runs):
                    Print.heading(f'Run {i+1}/{bench_parameters.runs}')
                    try:
                        self._run_single(
                            hosts, r, bench_parameters, node_parameters, debug
                        )
                        self._logs(hosts, faults, protocol, ddos).print(PathMaker.result_file(
                            n, r, bench_parameters.tx_size, faults
                        ))
                    except (subprocess.SubprocessError, GroupException, ParseError) as e:
                        self.kill(hosts=hosts)
                        if isinstance(e, GroupException):
                            e = FabricError(e)
                        Print.error(BenchError('Benchmark failed', e))
                        continue
