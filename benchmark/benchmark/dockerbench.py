from cmd import Cmd
from copy import copy
import io
from math import ceil
import os
from posixpath import basename, splitext
import tarfile
from time import sleep
from benchmark.config import Committee, Key, NodeParameters, BenchParameters, ConfigError
from benchmark.commands import CommandMaker
from benchmark.remote import FabricError
from benchmark.logs import LogParser, ParseError
from benchmark.utils import BenchError, PathMaker, Print, progress_bar

import docker
import subprocess

PATH_TO_BENCHMARK = "/SuperHotStuff/benchmark"
IMAGE = "superhotstuff"
SERVICE = "superhotstuff01"
HOSTNAME = "server-{}".format(SERVICE)
NETWORK = "benchNet"

def copy_from_container(container, local: str, dst: str, prefix = PATH_TO_BENCHMARK):
    """ local shall be an absolute path """
    # add the prefix to the path
    dst = os.path.join(prefix, dst)
    first = True
    with open(local, 'wb') as f:
        strm, _ = container.get_archive(dst)
        for d in strm:
            if first:
                d = d[512:]
                first = False
            f.write(d)

def copy_to_container(container, src: str, dst_dir: str = PATH_TO_BENCHMARK):
    """ src shall be an absolute path """
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode='w|') as tar, open(src, 'rb') as f:
        info = tar.gettarinfo(fileobj=f)
        info.name = os.path.basename(src)
        tar.addfile(info, f)
    
    container.put_archive(dst_dir, stream.getvalue())

def docker_cmd(cmd):
    return f'/bin/bash -c "{cmd}"'

class DockerBench:

    def __init__(self, bench_parameters_dict, node_parameters_dict, settings):
        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)
        self.docker_client = docker.from_env()
        self.settings = settings

    def __getattr__(self, attr):
        return getattr(self.bench_parameters, attr)
    
    def _background_run(self, container, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'tmux new -d -s "{name}" "{command} |& tee {log_file}"'
        container.exec_run(cmd)
    
    def kill(self):
        Print.info(f'Removing service {SERVICE} and killing containers')
        self._kill_containers()

    def _kill_containers(self):
        for service in self.docker_client.services.list():
            service.remove()
        # Wait for containers to be removed.
        while len(self.docker_client.containers.list()) > 0:
            sleep(1)
        return
    
    def stop(self, delete_logs=False):
        assert isinstance(delete_logs, bool)
        Print.info(f'Sending stop command, delete_logs={delete_logs}...')
        delete_logs = CommandMaker.clean_logs() if delete_logs else 'true'
        cmd = [delete_logs, f'({CommandMaker.kill()} || true)']
        cmd = docker_cmd(' && '.join(cmd))
        for container in self.docker_client.containers.list():
            container.exec_run(cmd)
    
    def _config(self, hosts, node_parameters):
        Print.info('Generating configuration files...')

        # Cleanup all local configuration files.
        cmd = CommandMaker.cleanup()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Generate configuration files.
        keys = []
        key_files = [PathMaker.key_file(i) for i in range(len(hosts))]
        for filename in key_files:
            cmd = CommandMaker.generate_key(filename).split()
            subprocess.run(cmd, check=True)
            keys += [Key.from_file(filename)]

        names = [x.name for x in keys]
        consensus_addr = [f'{x}:{self.settings["consensus_port"]}' for x in hosts]
        front_addr = [f'{x}:{self.settings["front_port"]}' for x in hosts]
        mempool_addr = [f'{x}:{self.settings["mempool_port"]}' for x in hosts]
        committee = Committee(names, consensus_addr, front_addr, mempool_addr)
        committee.print(PathMaker.committee_file())

        node_parameters.print(PathMaker.parameters_file())

        for container in self.docker_client.containers.list():
            # Cleanup all nodes.
            cmd = f'{CommandMaker.cleanup()} || true'
            container.exec_run(cmd)
            # Create alias for the client and nodes binary.
            cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
            container.exec_run(docker_cmd(cmd))

        # Upload configuration files.
        progress = progress_bar(self.docker_client.containers.list(), prefix='Uploading config files:')
        for i, container in enumerate(progress):
            copy_to_container(container, PathMaker.committee_file())
            copy_to_container(container, PathMaker.parameters_file())
            copy_to_container(container, PathMaker.key_file(i))

        return committee
    
    def _run_single(self, hosts, max_clients, rate, bench_parameters, node_parameters, debug=False):
        Print.info('Booting testbed...')
        self.stop(delete_logs=True)
        # Run the clients (they will wait for the nodes to be ready).
        # Filter all faulty nodes from the client addresses (or they will wait
        # for the faulty nodes to be online).
        committee = Committee.load(PathMaker.committee_file())
        addresses = [f'{x}:{self.settings["front_port"]}' for x in hosts]
        rate_share = ceil(rate / max_clients)  # Doesn't take faults into account.
        timeout = node_parameters.timeout_delay
        client_logs = [PathMaker.client_log_file(i) for i in range(len(hosts))]
        number_of_clients = 0
        for container, log_file in zip(self.docker_client.containers.list(), client_logs):
            if number_of_clients >= max_clients:
                rate_share = 0
            cmd = CommandMaker.run_client(
                f'127.0.0.1:{self.settings["front_port"]}',
                bench_parameters.tx_size,
                rate_share,
                timeout,
                nodes=addresses
            )
            self._background_run(container, cmd, log_file)
            number_of_clients += 1

        # Run the nodes.
        key_files = [PathMaker.key_file(i) for i in range(len(hosts))]
        dbs = [PathMaker.db_path(i) for i in range(len(hosts))]
        node_logs = [PathMaker.node_log_file(i) for i in range(len(hosts))]
        for container, key_file, db, log_file in zip(self.docker_client.containers.list(), key_files, dbs, node_logs):
            cmd = CommandMaker.run_node(
                key_file,
                PathMaker.committee_file(),
                db,
                PathMaker.parameters_file(),
                bench_parameters.topology.name,
                debug=debug
            )
            self._background_run(container, cmd, log_file)

        # Wait for the nodes to synchronize
        Print.info('Waiting for the nodes to synchronize...')
        sleep(2 * node_parameters.timeout_delay / 1000)

        # Wait for all transactions to be processed.
        duration = bench_parameters.duration
        for _ in progress_bar(range(20), prefix=f'Running benchmark ({duration} sec):'):
            sleep(ceil(duration / 20))
        self.stop()

    def _logs(self, faults):
        # Delete local logs (if any).
        cmd = CommandMaker.clean_logs()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Download log files.
        progress = progress_bar(self.docker_client.containers.list(), prefix='Downloading logs:')
        for i, container in enumerate(progress):
            copy_from_container(container, PathMaker.node_log_file(i), PathMaker.node_log_file(i))
            copy_from_container(container, PathMaker.client_log_file(i), PathMaker.client_log_file(i))
            
        # Parse logs and return the parser.
        Print.info('Parsing logs and computing performance...')
        return LogParser.process(PathMaker.logs_path(), faults=faults)

    def launch_containers(self, n):
        # Remove the previous service
        self.kill()

        # Create and replicate the services
        Print.info(f'Creating service {SERVICE}')                 
        self.docker_client.services.create(
            image = IMAGE,
            name = SERVICE,
            hostname = HOSTNAME,
            cap_add = ['NET_ADMIN'],
            labels = {SERVICE: 'true'},
            endpoint_spec = {'mode': 'dnsrr'},
            mode = {'replicated': {'replicas': n}},
            networks = [NETWORK],
        )
        Print.info("Waiting for the service to be ready...")
        # Wait for the containers to be ready
        stop_time = 120
        elapsed_time = 0
        while len(self.docker_client.containers.list()) < n:
            sleep(1)
            elapsed_time += 1
            if elapsed_time > stop_time:
                raise Exception('Containers not ready after 120 seconds')

        for container in self.docker_client.containers.list():
            while container.status != 'running':
                sleep(1)
                elapsed_time += 1
                if elapsed_time > stop_time:
                    raise Exception('Containers not ready after 120 seconds')

        # traffic control rules
        if self.latency > 0 or self.bandwidth != "":
            for container in self.docker_client.containers.list():
                cmd = CommandMaker.tc(self.latency, self.bandwidth)
                container.exec_run(docker_cmd(cmd))

    def run(self, debug = False):
        assert isinstance(debug, bool)
        Print.heading('Starting remote benchmark')

        # Run benchmarks.
        for i, n in enumerate(self.bench_parameters.nodes):
            for r in self.bench_parameters.rate:
                self.launch_containers(n)

                Print.heading(f'\nRunning {n} nodes (input rate: {r:,} tx/s)')

                Print.info(f'Containers : {self.docker_client.containers.list()}')
                # Get the ip addresses of the containers
                hosts = []
                for container in self.docker_client.containers.list():
                    hosts.append(container.attrs['NetworkSettings']['Networks']['benchNet']['IPAddress'])
                Print.info(f'hosts : {hosts}')

                clients = self.clients[i]

                # Upload all configuration files.
                try:
                    self._config(hosts, self.node_parameters)
                except (subprocess.SubprocessError) as e:
                    Print.error(BenchError('Failed to configure nodes', e))
                    continue
                
                # Do not boot faulty nodes.
                faults = self.bench_parameters.faults
                hosts = hosts[:n-faults]

                # Run the benchmark.
                for j in range(self.bench_parameters.runs):
                    Print.heading(f'Run {j+1}/{self.bench_parameters.runs}')
                    try:
                        self._run_single(
                            hosts, clients, r, self.bench_parameters, self.node_parameters, debug
                        )
                        # faults, nodes, rate, tx_size, latency, bandwidth, clients
                        self._logs(faults).print(PathMaker.result_file(
                            faults, n, r, self.bench_parameters.tx_size, self.latency, self.bandwidth if self.bandwidth != "" else "max", clients, self.topology.name
                        ))
                    except (subprocess.SubprocessError, ParseError) as e:
                        self.kill()
                        Print.error(BenchError('Benchmark failed', e))
                        continue
                self.kill()
