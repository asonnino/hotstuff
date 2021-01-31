from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from paramiko import RSAKey
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from time import sleep
import subprocess

from benchmark.config import Committee, Key, Parameters
from benchmark.utils import Print, PathMaker
from benchmark.commands import CommandMaker
from aws.instance import InstanceManager


class BenchError(Exception):
    def __init__(self, message, error):
        assert isinstance(error, GroupException)
        self.message = message
        self.cause = list(error.result.values())[0]
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
            raise BenchError('Failed to setup nodes', e)

    def _kill(self, hosts, clean_logs=False):
        clean_logs = 'rm -r logs ; mkdir -p logs' if clean_logs else 'true'
        cmd = [
            clean_logs,
            f'({CommandMaker.kill()} || true)'
        ]
        try:
            g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect_kwargs)
            g.run(' && '.join(cmd), hide=True)
        except GroupException as e:
            raise BenchError('Failed to kill nodes', e)

    def _update(self, hosts):
        cmd = [
            f'(cd {self.settings.repo_name} && git fetch)',
            f'(cd {self.settings.repo_name} && git pull)',
            f'(cd {self.settings.repo_name} && git checkout {self.settings.branch})',
            'source $HOME/.cargo/env',
            f'(cd {self.settings.repo_name}/node && {CommandMaker.compile()})',
            CommandMaker.alias_binaries(f'./{self.settings.repo_name}/target/release/')
        ]
        try:
            g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect_kwargs)
            g.run(' && '.join(cmd), hide=True)
        except GroupException as e:
            raise BenchError('Failed to update nodes', e)

    def _logs(self):
        pass

    def _config(self, debug=False):
        hosts = self.manager.hosts()

        # Kill any previous testbed and cleanup all files.
        self._kill(hosts, clean_logs=True)
        try:
            cmd = CommandMaker.cleanup()
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5) # Removing the store may take time.

            # Recompile the latest code.
            cmd = CommandMaker.compile().split()
            subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

            # Create alias for the client and nodes binary.
            cmd = CommandMaker.alias_binaries(PathMaker.binary_path)
            subprocess.run([cmd], shell=True)

            # Generate configuration files.
            keys = []
            key_files = [PathMaker.key_file(i) for i in range(len(hosts))]
            for filename in key_files:
                cmd = CommandMaker.generate_key(filename).split()
                subprocess.run(cmd, check=True)
                keys += [Key.from_file(filename)]
        except subprocess.SubprocessError as e:
            raise BenchError('Failed to print configuration files', e)

        names = [x.name for x in keys]
        consensus_addr = [f'{x}:{self.settings.consensus_port}' for x in hosts]
        mempool_addr = [f'{x}:{self.settings.mempool_port}' for x in hosts]
        front_addr = [f'{x}:{self.settings.front_port}' for x in hosts]
        committee = Committee(names, consensus_addr, mempool_addr, front_addr)
        committee.print(PathMaker.committee_file())

        parameters = Parameters.default()
        parameters.print(PathMaker.parameters_file())

        # Cleanup all nodes.
        g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect_kwargs)
        g.run(CommandMaker.cleanup(), hide=True)

        # Upload configuration files.
        for i, host in enumerate(hosts):
            print(f' [{i+1}/{len(hosts)}] Uploading config files...', end='\r')
            c = Connection(host, user='ubuntu', connect_kwargs=self.connect_kwargs)
            c.put(PathMaker.committee_file, '.')
            c.put(PathMaker.key_file(i), '.')
            c.put(PathMaker.parameters_file, '.')