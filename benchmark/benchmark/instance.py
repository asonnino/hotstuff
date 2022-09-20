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
        self.ssh_user = 'ubuntu'
        self.package_manager = 'apt-get'
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
        pool_2 = 'ocid1.instancepool.oc1.us-sanjose-1.aaaaaaaatjtnweiagxswuivz44c6ptivbtb4wl2sllwfjtm6codn5s6vfwoq'
        self.pool_id = pool_2

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
        # [print(i, x.display_name) for i, x in enumerate(result.data)]
        return [x.id for x in result.data]

    def hosts(self, flat=False):
        # compute_client = ComputeClient(self.config)
        # network_client = VirtualNetworkClient(self.config)

        # last_region = None
        # ips = defaultdict(set)
        # for id in self._get_instance_ids():
        #     vnic_id = compute_client.list_vnic_attachments(
        #         self.compartment_id, instance_id=id
        #     )
        #     result = network_client.get_vnic(vnic_id.data[0].vnic_id)
        #     region = result.data.availability_domain
        #     ip = result.data.public_ip
        #     ips[region].add(ip)
        #     last_region = region

        # Hack: There seems to be a non-deterministic bug in the OCI API making
        # it `forget` the last x machines.
        last_region = 'gDlt:US-SANJOSE-1-AD-1'
        ips = defaultdict(set)
        ips[last_region] = {
            '192.9.226.6',
            '192.18.143.75',
            '192.9.238.175',
            '152.67.234.2',
            '152.70.121.179',
            '192.9.234.154',
            '155.248.195.160',
            '150.230.46.24',
            '152.67.226.37',
            '192.9.151.192',
            '192.9.156.60',
            '152.67.224.87',
            '129.159.42.1',
            '192.9.147.253',
            '152.70.123.18',
            '192.9.128.243',
            '152.67.254.117',
            '138.2.234.97',
            '152.67.248.190',
            '192.9.153.160',
            '192.9.156.86',
            '150.230.40.128',
            '192.9.133.25',
            '192.9.133.246',
            '150.230.35.49',
            '192.18.141.164',
            '192.9.153.110',
            '155.248.210.54',
            '150.230.44.43',
            '192.18.128.119',
            '192.18.133.61',
            '138.2.227.228',
            '129.159.33.176',
            '192.9.128.25',
            '152.67.233.149',
            '192.9.137.18',
            '192.18.131.93',
            '192.9.151.75',
            '192.9.155.208',
            '192.9.132.231',
            '152.67.250.181',
            '138.2.237.19',
            '192.9.133.56',
            '192.9.247.107',
            '192.9.148.216',
            '192.9.245.233',
            '192.9.149.116',
            '192.9.234.87',
            '192.9.131.23',
            '152.70.113.174',
            '192.18.128.15',
            '192.18.136.127',
            '155.248.211.158',
            '192.18.130.128',
            '155.248.199.187',
            '152.67.235.205',
            '138.2.226.204',
            '192.9.129.209',
            '150.230.34.143',
            '192.9.132.155',
            '129.159.40.179',
            '192.9.244.78',
            '192.9.142.208',
            '192.9.132.156',
            '192.9.134.252',
            '152.67.251.41',
            '192.9.238.236',
            '152.70.115.77',
            '155.248.194.136',
            '138.2.229.24',
            '192.9.232.210',
            '138.2.224.128',
            '192.9.228.101',
            '152.70.117.199',
            '152.67.226.112',
            '138.2.233.250',
            '192.9.240.32',
            '150.230.38.178',
            '129.159.33.175',
            '152.67.252.63',
            '152.70.126.160',
            '138.2.236.94',
            '152.67.229.197',
            '192.9.250.220',
            '150.230.33.37',
            '192.18.136.225',
            '192.9.241.108',
            '192.18.128.134',
            '129.159.37.17',
            '192.18.140.117',
            '138.2.235.200',
            '192.18.137.208',
            '155.248.214.62',
            '152.67.230.120',
            '138.2.239.232',
            '155.248.198.29',
            '192.18.128.38',
            '152.67.252.101',
            '192.9.148.151',
            '155.248.204.7',

            '192.9.138.130',
            '192.9.157.81',
            '152.70.116.123',
            '152.70.118.29',
            '192.9.233.244',
            '192.18.133.34',
            '129.159.42.123',
            '192.18.142.15',
            '150.230.46.58',
            '192.9.158.71',
            '192.9.230.221',
            '192.9.231.82',
            '129.159.37.111',
            '150.230.36.123',
            '192.9.155.117',
            '138.2.238.67',
            '152.67.231.179',
            '138.2.230.14',
            '192.18.140.15',
            '192.9.144.58'
        }
        ips[last_region] = list(ips[last_region])
        # print(len(ips[last_region]))
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
                # print(f'\'{ip}\',')

        print(
            '\n'
            '----------------------------------------------------------------\n'
            ' INFO:\n'
            '----------------------------------------------------------------\n'
            f' Available machines: {sum(len(x) for x in hosts.values())}\n'
            f'{text}'
            '----------------------------------------------------------------\n'
        )
