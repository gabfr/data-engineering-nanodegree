import configparser
import pandas as pd
import boto3
import json
import time

KEY                    = None
SECRET                 = None

DWH_CLUSTER_TYPE       = None
DWH_NUM_NODES          = None
DWH_NODE_TYPE          = None

DWH_CLUSTER_IDENTIFIER = None
DWH_DB                 = None
DWH_DB_USER            = None
DWH_DB_PASSWORD        = None
DWH_PORT               = None

DWH_IAM_ROLE_NAME      = None


def config_parse_file():
    """
    Parse the dwh.cfg configuration file
    :return:
    """
    global KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES, \
        DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, \
        DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME

    print("Parsing the config file...")
    config = configparser.ConfigParser()
    with open('dwh.cfg') as configfile:
        config.read_file(configfile)

        KEY = config.get('AWS', 'KEY')
        SECRET = config.get('AWS', 'SECRET')

        DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
        DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
        DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

        DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
        DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")

        DWH_DB = config.get("CLUSTER", "DB_NAME")
        DWH_DB_USER = config.get("CLUSTER", "DB_USER")
        DWH_DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
        DWH_PORT = config.get("CLUSTER", "DB_PORT")


def create_iam_role(iam):
    """
    Create the AWS IAM role
    :param iam:
    :return:
    """
    global DWH_IAM_ROLE_NAME
    dwhRole = None
    try:
        print('1.1 Creating a new IAM Role')
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)
        dwhRole = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)
    return dwhRole


def attach_iam_role_policy(iam):
    """
    Attach the AmazonS3ReadOnlyAccess role policy to the created IAM
    :param iam:
    :return:
    """
    global DWH_IAM_ROLE_NAME
    print('1.2 Attaching Policy')
    return iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")['ResponseMetadata']['HTTPStatusCode'] == 200


def get_iam_role_arn(iam):
    """
    Get the IAM role ARN string
    :param iam: The IAM resource client
    :return:string
    """
    global DWH_IAM_ROLE_NAME
    return iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']


def start_cluster_creation(redshift, roleArn):
    """
    Start the Redshift cluster creation
    :param redshift: The redshift resource client
    :param roleArn: The created role ARN
    :return:
    """
    global DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, \
        DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD
    print("2. Starting redshift cluster creation")
    try:
        response = redshift.create_cluster(
            # HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            # Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            # Roles (for s3 access)
            IamRoles=[roleArn]
        )
        print("Redshift cluster creation http response status code: ")
        print(response['ResponseMetadata']['HTTPStatusCode'])
        return response['ResponseMetadata']['HTTPStatusCode'] == 200
    except Exception as e:
        print(e)
    return False


def config_persist_cluster_infos(redshift):
    """
    Write back to the dwh.cfg configuration file the cluster endpoint and IAM ARN
    :param redshift: The redshift resource client
    :return:
    """
    global DWH_CLUSTER_IDENTIFIER
    print("Writing the cluster address and IamRoleArn to the config file...")

    cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    config = configparser.ConfigParser()

    with open('dwh.cfg') as configfile:
        config.read_file(configfile)

    config.set("CLUSTER", "HOST", cluster_props['Endpoint']['Address'])
    config.set("IAM_ROLE", "ARN", cluster_props['IamRoles'][0]['IamRoleArn'])

    with open('dwh.cfg', 'w+') as configfile:
        config.write(configfile)

    config_parse_file()


def get_redshift_cluster_status(redshift):
    """
    Retrieves the Redshift cluster status
    :param redshift: The Redshift resource client
    :return: The cluster status
    """
    global DWH_CLUSTER_IDENTIFIER
    cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    cluster_status = cluster_props['ClusterStatus']
    return cluster_status.lower()


def check_cluster_creation(redshift):
    """
    Check if the cluster status is available, if it is returns True. Otherwise, false.
    :param redshift: The Redshift client resource
    :return:bool
    """
    if get_redshift_cluster_status(redshift) == 'available':
        return True
    return False


def destroy_redshift_cluster(redshift):
    """
    Destroy the Redshift cluster (request deletion)
    :param redshift: The Redshift client resource
    :return:None
    """
    global DWH_CLUSTER_IDENTIFIER
    redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)


def aws_open_redshift_port(ec2, redshift):
    """
    Opens the Redshift port on the VPC security group.
    :param ec2: The EC2 client resource
    :param redshift: The Redshift client resource
    :return:None
    """
    global DWH_CLUSTER_IDENTIFIER, DWH_PORT
    cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    try:
        vpc = ec2.Vpc(id=cluster_props['VpcId'])
        all_security_groups = list(vpc.security_groups.all())
        print(all_security_groups)
        defaultSg = all_security_groups[1]
        print(defaultSg)

        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)


def aws_resource(name, region):
    """
    Creates an AWS client resource
    :param name: The name of the resource
    :param region: The region of the resource
    :return:
    """
    global KEY, SECRET
    return boto3.resource(name, region_name=region, aws_access_key_id=KEY, aws_secret_access_key=SECRET)


def aws_client(service, region):
    """
    Creates an AWS client
    :param service: The service
    :param region: The region of the service
    :return:
    """
    global KEY, SECRET
    return boto3.client(service, aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name=region)

def main():
    config_parse_file()

    # ec2 = aws_resource('ec2', 'us-east-2')
    # s3 = aws_resource('s3', 'us-west-2')
    iam = aws_client('iam', "us-east-2")
    redshift = aws_client('redshift', "us-east-2")

    create_iam_role(iam)
    attach_iam_role_policy(iam)
    roleArn = get_iam_role_arn(iam)

    clusterCreationStarted = start_cluster_creation(redshift, roleArn)

    if clusterCreationStarted:
        print("The cluster is being created.")
        # while True:
        #     print("Gonna check if the cluster was created...")
        #     if check_cluster_creation(redshift):
        #         config_persist_cluster_infos(redshift)
        #         aws_open_redshift_port(ec2, redshift)
        #         break
        #     else:
        #         print("Not yet. Waiting 30s before next check.")
        #     time.sleep(30)
        # print("DONE!!")

        # wait until  becomes true?

if __name__ == '__main__':
    main()