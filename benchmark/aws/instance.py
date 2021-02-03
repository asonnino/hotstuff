import boto3
from botocore.exceptions import ClientError

from benchmark.utils import Print, BenchError
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

    def __init__(self, settings):
        assert isinstance(settings, Settings)
        self.settings = settings
        # TODO: Support for WAN bench.
        region = settings.aws_regions[0]
        self.client = boto3.client('ec2', region_name=region)

    @classmethod
    def make(cls, settings_file='settings.json'):
        try:
            return cls(Settings.load(settings_file))
        except SettingsError as e:
            raise BenchError('Failed to load settings', e)

    def _create_security_group(self):
        self.client.create_security_group(
            Description='HotStuff node',
            GroupName=self.SECURITY_GROUP_NAME,
        )

        self.client.authorize_security_group_ingress(
            GroupName=self.SECURITY_GROUP_NAME,
            IpPermissions=[
                {
                    'IpProtocol': 'tcp',
                    'FromPort': 22,
                    'ToPort': 22,
                    'IpRanges': [{
                        'CidrIp': '0.0.0.0/0',
                        'Description': 'Debug SSH access',
                    }],
                    'Ipv6Ranges': [{
                        'CidrIpv6': '::/0',
                        'Description': 'Debug SSH access',
                    }],
                },
                {
                    'IpProtocol': 'tcp',
                    'FromPort': self.settings.consensus_port,
                    'ToPort': self.settings.consensus_port,
                    'IpRanges': [{
                        'CidrIp': '0.0.0.0/0',
                        'Description': 'Consensus port',
                    }],
                    'Ipv6Ranges': [{
                        'CidrIpv6': '::/0',
                        'Description': 'Consensus port',
                    }],
                },
                {
                    'IpProtocol': 'tcp',
                    'FromPort': self.settings.mempool_port,
                    'ToPort': self.settings.mempool_port,
                    'IpRanges': [{
                        'CidrIp': '0.0.0.0/0',
                        'Description': 'Mempool port',
                    }],
                    'Ipv6Ranges': [{
                        'CidrIpv6': '::/0',
                        'Description': 'Mempool port',
                    }],
                },
                {
                    'IpProtocol': 'tcp',
                    'FromPort': self.settings.front_port,
                    'ToPort': self.settings.front_port,
                    'IpRanges': [{
                        'CidrIp': '0.0.0.0/0',
                        'Description': 'Front end to accept clients transactions',
                    }],
                    'Ipv6Ranges': [{
                        'CidrIpv6': '::/0',
                        'Description': 'Front end to accept clients transactions',
                    }],
                },
            ]
        )

    def create_instances(self, instances):
        try:
            self._create_security_group()
        except ClientError as e:
            error = AWSError(e)
            if error.code != 'InvalidGroup.Duplicate':
                raise BenchError('Failed to create security group', error)

        try:
            self.client.run_instances(
                ImageId='ami-0885b1f6bd170450c', # Ubuntu 20.04
                InstanceType=self.settings.instance_type,
                KeyName='aws',
                MaxCount=instances,
                MinCount=instances,
                SecurityGroups=[self.SECURITY_GROUP_NAME],
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
            )
            Print.info(f'Successfully created {instances} new instances')
        except ClientError as e:
            raise BenchError('Failed to create AWS instances', AWSError(e))

    def _get(self, status):
        assert isinstance(status, list)
        ok = {'pending', 'running', 'shutting-down', 'terminated', 'stopping', 'stopped'} 
        assert all(x in ok for x in status)

        response = self.client.describe_instances(
            Filters=[
                {
                    'Name': 'tag:Name',
                    'Values': [self.INSTANCE_NAME]
                },
                {
                    'Name': 'instance-state-name',
                    'Values': status
                }
            ]
        )
        instances = [y for x in response['Reservations'] for y in x['Instances']]
        ids = [x['InstanceId'] for x in instances]
        ips = [x['PublicIpAddress'] for x in instances if 'PublicIpAddress' in x]
        return ids, ips

    def terminate_instances(self):
        # NOTE: We do not delete the security group because we would have to wait
        # until all instances are terminated before attempting its deletion. 
        try:
            ids, _ = self._get(['pending', 'running', 'stopping', 'stopped'])
            if ids:
                self.client.terminate_instances(InstanceIds=ids)
            Print.heading(f'Testbed of {len(ids)} instances successfully destroyed')
        except ClientError as e:
            raise BenchError('Failed to terminate instances', AWSError(e))
        
    def start_instances(self):
        try:
            ids, _ = self._get(['stopping', 'stopped'])
            if ids:
                self.client.start_instances(InstanceIds=ids)
            Print.heading(f'Starting {len(ids)} instances')
        except ClientError as e:
            raise BenchError('Failed to start instances', AWSError(e))
        

    def stop_instances(self):
        try:
            ids, _ = self._get(['pending', 'running'])
            if ids:
                self.client.stop_instances(InstanceIds=ids)
            Print.heading(f'Stopping {len(ids)} instances')
        except ClientError as e:
            raise BenchError(AWSError(e))
        
    def print_info(self):
        try:
            _, ips = self._get(['pending', 'running'])
        except ClientError as e:
            raise BenchError('Failed to print instances info', AWSError(e))

        key = self.settings.key_path
        text = ''
        for i, host in enumerate(ips):
            new_line = '\n' if (i+1) % 5 == 0 else ''
            text += f' {i}\tssh -i {key} ubuntu@{host}\n{new_line}'
        print(
            '\n'
            '-------------------------------------------------------------------\n'
            ' INFO:\n'
            '-------------------------------------------------------------------\n'
            f' Total available machines: {len(ips)}\n'
            '\n'
            f'{text}'
            '-------------------------------------------------------------------\n'
        )

    def hosts(self):
        try:
            _, ips = self._get(['pending', 'running'])
            return ips
        except ClientError as e:
            raise BenchError('Failed to gather instances ip addresses', AWSError(e))