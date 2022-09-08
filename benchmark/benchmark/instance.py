from collections import defaultdict, OrderedDict
from time import sleep
from oci.config import validate_config
from oci.core import ComputeManagementClient, ComputeClient, VirtualNetworkClient

from benchmark.utils import Print, BenchError, progress_bar
from benchmark.settings import Settings, SettingsError


# class AWSError(Exception):
#     def __init__(self, error):
#         assert isinstance(error, ClientError)
#         self.message = error.response['Error']['Message']
#         self.code = error.response['Error']['Code']
#         super().__init__(self.message)


class InstanceManager:
    def __init__(self, settings):
        assert isinstance(settings, Settings)
        self.settings = settings
        self.ssh_user = 'opc'
        self.package_manager = 'yum'
        # self.clients = OrderedDict()
        # for region in settings.aws_regions:
        #     self.clients[region] = boto3.client('ec2', region_name=region)

        self.compartment_id = "ocid1.tenancy.oc1..aaaaaaaak5urycwdhjmdgdkfgu3q7wzamv63w6qa6pf4o5ry2dulte6aos4q"
        config = {
            "user": "ocid1.user.oc1..aaaaaaaakturkk3huvbnt6bk64cpi2ffr7t5emxoff2h4xai2unghk33tlra",
            "key_file": "/Users/alberto/.ssh/oci-2.pem",
            "fingerprint": "01:3f:f7:e0:ab:32:06:d8:74:2c:39:d0:bb:81:be:0f",
            "tenancy": self.compartment_id,
            "region": "us-sanjose-1"
        }
        validate_config(config)
        self.config = config
        self.pool_id = "ocid1.instancepool.oc1.us-sanjose-1.aaaaaaaafmhldzoruib5q4fva5zzcsfk2hg7slggjuinuunt6ekapkolihnq"

    @classmethod
    def make(cls, settings_file='settings.json'):
        try:
            return cls(Settings.load(settings_file))
        except SettingsError as e:
            raise BenchError('Failed to load settings', e)

    # def _get(self, state):
    #     # Possible states are: 'pending', 'running', 'shutting-down',
    #     # 'terminated', 'stopping', and 'stopped'.
    #     ids, ips = defaultdict(list), defaultdict(list)
    #     for region, client in self.clients.items():
    #         r = client.describe_instances(
    #             Filters=[
    #                 {
    #                     'Name': 'tag:Name',
    #                     'Values': [self.settings.testbed]
    #                 },
    #                 {
    #                     'Name': 'instance-state-name',
    #                     'Values': state
    #                 }
    #             ]
    #         )
    #         instances = [y for x in r['Reservations'] for y in x['Instances']]
    #         for x in instances:
    #             ids[region] += [x['InstanceId']]
    #             if 'PublicIpAddress' in x:
    #                 ips[region] += [x['PublicIpAddress']]
    #     return ids, ips

    # def _wait(self, state):
    #     # Possible states are: 'pending', 'running', 'shutting-down',
    #     # 'terminated', 'stopping', and 'stopped'.
    #     while True:
    #         sleep(1)
    #         ids, _ = self._get(state)
    #         if sum(len(x) for x in ids.values()) == 0:
    #             break

    # def _create_security_group(self, client):
    #     client.create_security_group(
    #         Description='HotStuff node',
    #         GroupName=self.settings.testbed,
    #     )

    #     client.authorize_security_group_ingress(
    #         GroupName=self.settings.testbed,
    #         IpPermissions=[
    #             {
    #                 'IpProtocol': 'tcp',
    #                 'FromPort': 22,
    #                 'ToPort': 22,
    #                 'IpRanges': [{
    #                     'CidrIp': '0.0.0.0/0',
    #                     'Description': 'Debug SSH access',
    #                 }],
    #                 'Ipv6Ranges': [{
    #                     'CidrIpv6': '::/0',
    #                     'Description': 'Debug SSH access',
    #                 }],
    #             },
    #             {
    #                 'IpProtocol': 'tcp',
    #                 'FromPort': self.settings.consensus_port,
    #                 'ToPort': self.settings.consensus_port,
    #                 'IpRanges': [{
    #                     'CidrIp': '0.0.0.0/0',
    #                     'Description': 'Consensus port',
    #                 }],
    #                 'Ipv6Ranges': [{
    #                     'CidrIpv6': '::/0',
    #                     'Description': 'Consensus port',
    #                 }],
    #             },
    #             {
    #                 'IpProtocol': 'tcp',
    #                 'FromPort': self.settings.mempool_port,
    #                 'ToPort': self.settings.mempool_port,
    #                 'IpRanges': [{
    #                     'CidrIp': '0.0.0.0/0',
    #                     'Description': 'Mempool port',
    #                 }],
    #                 'Ipv6Ranges': [{
    #                     'CidrIpv6': '::/0',
    #                     'Description': 'Mempool port',
    #                 }],
    #             },
    #             {
    #                 'IpProtocol': 'tcp',
    #                 'FromPort': self.settings.front_port,
    #                 'ToPort': self.settings.front_port,
    #                 'IpRanges': [{
    #                     'CidrIp': '0.0.0.0/0',
    #                     'Description': 'Front end to accept clients transactions',
    #                 }],
    #                 'Ipv6Ranges': [{
    #                     'CidrIpv6': '::/0',
    #                     'Description': 'Front end to accept clients transactions',
    #                 }],
    #             },
    #         ]
    #     )

    # def _get_ami(self, client):
    #     # The AMI changes with regions.
    #     response = client.describe_images(
    #         Filters=[{
    #             'Name': 'description',
    #             'Values': ['Canonical, Ubuntu, 20.04 LTS, amd64 focal image build on 2020-10-26']
    #         }]
    #     )
    #     return response['Images'][0]['ImageId']

    def create_instances(self, instances):
        #     assert isinstance(instances, int) and instances > 0

        #     # Create the security group in every region.
        #     for client in self.clients.values():
        #         try:
        #             self._create_security_group(client)
        #         except ClientError as e:
        #             error = AWSError(e)
        #             if error.code != 'InvalidGroup.Duplicate':
        #                 raise BenchError('Failed to create security group', error)

        #     try:
        #         # Create all instances.
        #         size = instances * len(self.clients)
        #         progress = progress_bar(
        #             self.clients.values(), prefix=f'Creating {size} instances'
        #         )
        #         for client in progress:
        #             client.run_instances(
        #                 ImageId=self._get_ami(client),
        #                 InstanceType=self.settings.instance_type,
        #                 KeyName=self.settings.key_name,
        #                 MaxCount=instances,
        #                 MinCount=instances,
        #                 SecurityGroups=[self.settings.testbed],
        #                 TagSpecifications=[{
        #                     'ResourceType': 'instance',
        #                     'Tags': [{
        #                         'Key': 'Name',
        #                         'Value': self.settings.testbed
        #                     }]
        #                 }],
        #                 EbsOptimized=True,
        #                 BlockDeviceMappings=[{
        #                     'DeviceName': '/dev/sda1',
        #                     'Ebs': {
        #                         'VolumeType': 'gp2',
        #                         'VolumeSize': 200,
        #                         'DeleteOnTermination': True
        #                     }
        #                 }],
        #             )

        #         # Wait for the instances to boot.
        #         Print.info('Waiting for all instances to boot...')
        #         self._wait(['pending'])
        #         Print.heading(f'Successfully created {size} new instances')
        #     except ClientError as e:
        #         raise BenchError('Failed to create AWS instances', AWSError(e))
        raise NotImplementedError

    def terminate_instances(self):
        #     try:
        #         ids, _ = self._get(['pending', 'running', 'stopping', 'stopped'])
        #         size = sum(len(x) for x in ids.values())
        #         if size == 0:
        #             Print.heading(f'All instances are shut down')
        #             return

        #         # Terminate instances.
        #         for region, client in self.clients.items():
        #             if ids[region]:
        #                 client.terminate_instances(InstanceIds=ids[region])

        #         # Wait for all instances to properly shut down.
        #         Print.info('Waiting for all instances to shut down...')
        #         self._wait(['shutting-down'])
        #         for client in self.clients.values():
        #             client.delete_security_group(
        #                 GroupName=self.settings.testbed
        #             )

        #         Print.heading(f'Testbed of {size} instances destroyed')
        #     except ClientError as e:
        #         raise BenchError('Failed to terminate instances', AWSError(e))
        raise NotImplementedError

    def start_instances(self, max):
        #     size = 0
        #     try:
        #         ids, _ = self._get(['stopping', 'stopped'])
        #         for region, client in self.clients.items():
        #             if ids[region]:
        #                 target = ids[region]
        #                 target = target if len(target) < max else target[:max]
        #                 size += len(target)
        #                 client.start_instances(InstanceIds=target)
        #         Print.heading(f'Starting {size} instances')
        #     except ClientError as e:
        #         raise BenchError('Failed to start instances', AWSError(e))
        client = ComputeManagementClient(self.config)

        result = client.get_instance_pool(self.pool_id)
        if result.data.lifecycle_state == 'RUNNING':
            Print.heading(f'All {result.data.size} instances already started')
            return

        Print.heading('Starting instances')
        client.start_instance_pool(self.pool_id)
        while True:
            sleep(1)
            result = client.get_instance_pool(self.pool_id)
            if result.data.lifecycle_state == 'RUNNING':
                size = result.data.size
                Print.heading(f'Successfully started {size} instances')
                break

    def stop_instances(self):
        #     try:
        #         ids, _ = self._get(['pending', 'running'])
        #         for region, client in self.clients.items():
        #             if ids[region]:
        #                 client.stop_instances(InstanceIds=ids[region])
        #         size = sum(len(x) for x in ids.values())
        #         Print.heading(f'Stopping {size} instances')
        #     except ClientError as e:
        #         raise BenchError(AWSError(e))
        client = ComputeManagementClient(self.config)

        Print.heading('Stopping instances')
        client.stop_instance_pool(self.pool_id)
        while True:
            sleep(1)
            result = client.get_instance_pool(self.pool_id)
            if result.data.lifecycle_state == 'STOPPED':
                size = result.data.size
                Print.heading(f'Successfully stopped {size} instances')
                break

    def _get_instance_ids(self):
        client = ComputeManagementClient(self.config)
        result = client.list_instance_pool_instances(
            self.compartment_id, self.pool_id
        )
        return [x.id for x in result.data]

    def hosts(self, flat=False):
        # # 16 nodes in 'hotstuff-pool'.
        # ips = {'AD-1': [
        #     '192.9.234.191',
        #     '138.2.229.113',
        #     '192.9.130.156',
        #     '192.18.129.86',
        #     '192.9.146.219',
        #     '192.9.146.176',
        #     '138.2.233.236',
        #     '155.248.215.106',
        #     '155.248.202.199',
        #     '192.9.242.186',
        #     '138.2.231.231',
        #     '155.248.212.126',
        #     '192.18.140.43',
        #     '152.67.225.17',
        #     '138.2.231.70',
        #     '150.230.35.49',
        # ]}

        compute_client = ComputeClient(self.config)
        network_client = VirtualNetworkClient(self.config)

        ips = defaultdict(list)
        for id in self._get_instance_ids():
            vnic_id = compute_client.list_vnic_attachments(
                self.compartment_id, instance_id=id
            )
            result = network_client.get_vnic(vnic_id.data[0].vnic_id)
            region = result.data.availability_domain
            ip = result.data.public_ip
            ips[region] += [ip]

        return [x for y in ips.values() for x in y] if flat else ips

    def private_hosts(self, flat=False):
        compute_client = ComputeClient(self.config)
        network_client = VirtualNetworkClient(self.config)

        ips = defaultdict(list)
        for id in self._get_instance_ids():
            vnic_id = compute_client.list_vnic_attachments(
                self.compartment_id, instance_id=id
            )
            result = network_client.get_vnic(vnic_id.data[0].vnic_id)
            region = result.data.availability_domain
            ip = result.data.private_ip
            ips[region] += [ip]

        return [x for y in ips.values() for x in y] if flat else ips

    def print_info(self):
        hosts = self.hosts()
        key = self.settings.key_path
        text = ''
        for region, ips in hosts.items():
            text += f'\n Region: {region.upper()}\n'
            for i, ip in enumerate(ips):
                new_line = '\n' if (i+1) % 6 == 0 else ''
                text += f'{new_line} {i}\tssh -i {key} {self.ssh_user}@{ip}\n'
        print(
            '\n'
            '----------------------------------------------------------------\n'
            ' INFO:\n'
            '----------------------------------------------------------------\n'
            f' Available machines: {sum(len(x) for x in hosts.values())}\n'
            f'{text}'
            '----------------------------------------------------------------\n'
        )
