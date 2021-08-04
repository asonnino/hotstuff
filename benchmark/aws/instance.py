import boto3
from botocore.exceptions import ClientError
from collections import defaultdict, OrderedDict
from time import sleep

from benchmark.utils import Print, BenchError, progress_bar
from aws.settings import Settings, SettingsError


class AWSError(Exception):
    def __init__(self, error):
        assert isinstance(error, ClientError)
        self.message = error.response['Error']['Message']
        self.code = error.response['Error']['Code']
        super().__init__(self.message)


class InstanceManager:
    INSTANCE_NAME = 'hotstuff-node'
    SECURITY_GROUP_NAME = 'hotstuff'
    VPC_NAME = 'hotstuff'

    def __init__(self, settings):
        assert isinstance(settings, Settings)
        self.settings = settings
        self.clients = OrderedDict()
        for region in settings.aws_regions:
            self.clients[region] = boto3.client('ec2', region_name=region)

    @classmethod
    def make(cls, settings_file='settings.json'):
        try:
            return cls(Settings.load(settings_file))
        except SettingsError as e:
            raise BenchError('Failed to load settings', e)

    def _get(self, state):
        # Possible states are: 'pending', 'running', 'shutting-down',
        # 'terminated', 'stopping', and 'stopped'.
        ids, ips = defaultdict(list), defaultdict(list)
        for region, client in self.clients.items():
            r = client.describe_instances(
                Filters=[
                    {
                        'Name': 'tag:Name',
                        'Values': [self.INSTANCE_NAME]
                    },
                    {
                        'Name': 'instance-state-name',
                        'Values': state
                    }
                ]
            )
            instances = [y for x in r['Reservations'] for y in x['Instances']]
            for x in instances:
                ids[region] += [x['InstanceId']]
                if 'PublicIpAddress' in x:
                    ips[region] += [x['PublicIpAddress']]
        return ids, ips

    def _wait(self, state):
        # Possible states are: 'pending', 'running', 'shutting-down',
        # 'terminated', 'stopping', and 'stopped'.
        while True:
            sleep(1)
            ids, _ = self._get(state)
            if sum(len(x) for x in ids.values()) == 0:
                break

    def _create_vpc(self, region, index):
        ec2 = boto3.resource('ec2', region_name=region)

        vpc = ec2.create_vpc(CidrBlock='192.168.0.0/16')
        vpc.create_tags(
            Tags=[{
                'Key': 'Name',
                'Value': f'{self.VPC_NAME}-{region}-{index}'
            }]
        )
        vpc.wait_until_available()

        ig = ec2.create_internet_gateway()
        vpc.attach_internet_gateway(InternetGatewayId=ig.id)

        subnet = ec2.create_subnet(CidrBlock='192.168.1.0/24', VpcId=vpc.id)

        route_table = vpc.create_route_table()
        route_table.create_route(
            DestinationCidrBlock='0.0.0.0/0', GatewayId=ig.id
        )
        route_table.associate_with_subnet(SubnetId=subnet.id)

        sec_group = ec2.create_security_group(
            GroupName=f'{self.SECURITY_GROUP_NAME}-{vpc.id}',
            Description='HotStuff node',
            VpcId=vpc.id
        )
        sec_group.authorize_ingress(
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=22,
            ToPort=22
        )
        sec_group.authorize_ingress(
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=self.settings.consensus_port,
            ToPort=self.settings.consensus_port
        )
        sec_group.authorize_ingress(
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=self.settings.mempool_port,
            ToPort=self.settings.mempool_port
        )
        sec_group.authorize_ingress(
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=self.settings.front_port,
            ToPort=self.settings.front_port
        )

        return subnet.id, sec_group.id

    def _get_vpc_info(self, client):
        vpc_ids = []
        for x in client.describe_vpcs()['Vpcs']:
            if 'Tags' in x and self.VPC_NAME in str(x['Tags']):
                vpc_ids += [x['VpcId']]

        subnet_ids = {}
        for x in client.describe_subnets()['Subnets']:
            if x['VpcId'] in vpc_ids:
                subnet_ids[x['VpcId']] = x['SubnetId']

        sec_group_ids = {}
        for x in client.describe_security_groups()['SecurityGroups']:
            if x['VpcId'] in vpc_ids:
                sec_group_ids[x['VpcId']] = x['GroupId']

        info = {}
        for vpc_id, subnet_id in subnet_ids.items():
            assert vpc_id in sec_group_ids
            info[subnet_id] = sec_group_ids[vpc_id]

        return info

    def _get_ami(self, client):
        # The AMI changes with regions.
        response = client.describe_images(
            Filters=[{
                'Name': 'description',
                'Values': ['Canonical, Ubuntu, 20.04 LTS, amd64 focal image build on 2020-10-26']
            }]
        )
        return response['Images'][0]['ImageId']

    def create_instances(self, instances):
        assert isinstance(instances, int) and instances > 0

        # Ensure there are no other testbeds.
        try:
            ids, _ = self._get(['pending', 'running', 'stopping', 'stopped'])
        except ClientError as e:
            raise BenchError('Failed to get AWS account state', AWSError(e))

        if len(ids) != 0:
            Print.warn('Destroy the current testbed before creating a new one')
            return

        # Create all instances.
        try:
            size = instances * len(self.clients)
            progress = progress_bar(
                self.clients.items(), prefix=f'Creating {size} instances'
            )
            for region, client in progress:
                info = self._get_vpc_info(client)

                # Create missing VPCs and Subnets.
                if len(info) < instances:
                    for i in range(len(info), instances):
                        subnet_id, sec_group_id = self._create_vpc(region, i)
                        info[subnet_id] = sec_group_id

                # Run the instances.
                for subnet_id, sec_group_id in info.items():
                    client.run_instances(
                        ImageId=self._get_ami(client),
                        InstanceType=self.settings.instance_type,
                        KeyName=self.settings.key_name,
                        MaxCount=1,
                        MinCount=1,
                        # SecurityGroups=[self.SECURITY_GROUP_NAME],
                        TagSpecifications=[{
                            'ResourceType': 'instance',
                            'Tags': [{
                                'Key': 'Name',
                                'Value': self.INSTANCE_NAME
                            }]
                        }],
                        EbsOptimized=True,
                        BlockDeviceMappings=[{
                            'DeviceName': '/dev/sda1',
                            'Ebs': {
                                'VolumeType': 'gp2',
                                'VolumeSize': 200,
                                'DeleteOnTermination': True
                            }
                        }],
                        NetworkInterfaces=[{
                            'SubnetId': subnet_id,
                            'AssociatePublicIpAddress': True,
                            'DeviceIndex': 0,
                            'Groups': [sec_group_id],
                        }],
                    )

            # Wait for the instances to boot.
            Print.info('Waiting for all instances to boot...')
            self._wait(['pending'])
            Print.heading(f'Successfully created {size} new instances')
        except ClientError as e:
            raise BenchError('Failed to create AWS instances', AWSError(e))

    def terminate_instances(self):
        try:
            ids, _ = self._get(['pending', 'running', 'stopping', 'stopped'])
            size = sum(len(x) for x in ids.values())
            if size == 0:
                Print.heading(f'All instances are shut down')
                return

            # Terminate instances.
            for region, client in self.clients.items():
                if ids[region]:
                    client.terminate_instances(InstanceIds=ids[region])

            # Wait for all instances to properly shut down.
            Print.info('Waiting for all instances to shut down...')
            self._wait(['shutting-down'])
            Print.heading(f'Testbed of {size} instances destroyed')
        except ClientError as e:
            raise BenchError('Failed to terminate instances', AWSError(e))

    def start_instances(self, max):
        size = 0
        try:
            ids, _ = self._get(['stopping', 'stopped'])
            for region, client in self.clients.items():
                if ids[region]:
                    target = ids[region]
                    target = target if len(target) < max else target[:max]
                    size += len(target)
                    client.start_instances(InstanceIds=target)
            Print.heading(f'Starting {size} instances')
        except ClientError as e:
            raise BenchError('Failed to start instances', AWSError(e))

    def stop_instances(self):
        try:
            ids, _ = self._get(['pending', 'running'])
            for region, client in self.clients.items():
                if ids[region]:
                    client.stop_instances(InstanceIds=ids[region])
            size = sum(len(x) for x in ids.values())
            Print.heading(f'Stopping {size} instances')
        except ClientError as e:
            raise BenchError(AWSError(e))

    def hosts(self, flat=False):
        try:
            _, ips = self._get(['pending', 'running'])
            return [x for y in ips.values() for x in y] if flat else ips
        except ClientError as e:
            raise BenchError('Failed to gather instances IPs', AWSError(e))

    def print_info(self):
        hosts = self.hosts()
        key = self.settings.key_path
        text = ''
        for region, ips in hosts.items():
            text += f'\n Region: {region.upper()}\n'
            for i, ip in enumerate(ips):
                new_line = '\n' if (i+1) % 6 == 0 else ''
                text += f'{new_line} {i}\tssh -i {key} ubuntu@{ip}\n'
        print(
            '\n'
            '----------------------------------------------------------------\n'
            ' INFO:\n'
            '----------------------------------------------------------------\n'
            f' Available machines: {sum(len(x) for x in hosts.values())}\n'
            f'{text}'
            '----------------------------------------------------------------\n'
        )
