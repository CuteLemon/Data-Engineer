import pandas as pd
import boto3
import json
import configparser
from botocore.exceptions import ClientError
import time
import argparse

class Aws(object):
    """
    
    """

    def __init__(self, cfg_path):
        config = configparser.ConfigParser()
        config.read_file(open(cfg_path))

        self.KEY                    = config.get('AWS','KEY')
        self.SECRET                 = config.get('AWS','SECRET')

        self.DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
        self.DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
        self.DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

        self.DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
        self.DWH_DB                 = config.get("DWH","DWH_DB")
        self.DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
        self.DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
        self.DWH_PORT               = config.get("DWH","DWH_PORT")

        self.DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

        self.ec2 = boto3.resource('ec2',
                    region_name="us-west-2",
                    aws_access_key_id=self.KEY,
                    aws_secret_access_key=self.SECRET
                    )

        self.s3 = boto3.resource('s3',
                            region_name="us-west-2",
                            aws_access_key_id=self.KEY,
                            aws_secret_access_key=self.SECRET
                        )

        self.iam = boto3.client('iam',aws_access_key_id=self.KEY,
                            aws_secret_access_key=self.SECRET,
                            region_name='us-west-2'
                        )

        self.redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=self.KEY,
                            aws_secret_access_key=self.SECRET
                            )
    
    #1.1 Create the role, 
    def create_IAM(self):
        try:
            print("1.1 Creating a new IAM Role") 
            dwhRole = self.iam.create_role(
                Path='/',
                RoleName=self.DWH_IAM_ROLE_NAME,
                Description = "Allows Redshift clusters to call AWS services on your behalf.",
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}}],
                    'Version': '2012-10-17'})
            )    
        except Exception as e:
            print(e)

    # create cluster
    def create_cluster(self):
        print("1.2 Attaching Policy")

        self.iam.attach_role_policy(RoleName=self.DWH_IAM_ROLE_NAME,
                            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                            )['ResponseMetadata']['HTTPStatusCode']

        print("1.3 Get the IAM role ARN")
        roleArn = self.iam.get_role(RoleName=self.DWH_IAM_ROLE_NAME)['Role']['Arn']

        print(roleArn)
        try:
            response = self.redshift.create_cluster(        
                #HW
                ClusterType=self.DWH_CLUSTER_TYPE,
                NodeType=self.DWH_NODE_TYPE,
                NumberOfNodes=int(self.DWH_NUM_NODES),

                #Identifiers & Credentials
                DBName=self.DWH_DB,
                ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER,
                MasterUsername=self.DWH_DB_USER,
                MasterUserPassword=self.DWH_DB_PASSWORD,
                
                #Roles (for s3 access)
                IamRoles=[roleArn]  
            )
        except Exception as e:
            print(e)
        props = self.redshift.describe_clusters(ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        # IP config
        try:
            vpc = self.ec2.Vpc(id=props['VpcId'])
            defaultSg = list(vpc.security_groups.all())[0]
            print(defaultSg)
            defaultSg.authorize_ingress(
                GroupName=defaultSg.group_name,
                CidrIp='0.0.0.0/0',
                IpProtocol='TCP',
                FromPort=int(self.DWH_PORT),
                ToPort=int(self.DWH_PORT)
            )
        except Exception as e:
            print(e)

    # check if cluster is available
    def get_cluster_state(self):
        pd.set_option('display.max_colwidth', -1)
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
        props = self.redshift.describe_clusters(ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER)['Clusters'][0] 
        x = [(k, v) for k,v in props.items() if k in keysToShow]
        data = pd.DataFrame(data=x, columns=["Key", "Value"])   
        print(data) 

    def delete_cluster(self):
        print('deleting cluster...')    
        self.redshift.delete_cluster( ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True) 

if __name__ == "__main__":
# command line control
    parser = argparse.ArgumentParser()
    parser.print_help()
    parser.add_argument('-create',required=False, action="store_true",help='create cluster')
    parser.add_argument('-state',required=False, action="store_true",help='cluster state')
    parser.add_argument('-delete',required=False, action="store_true",help='delete cluster')
    args = parser.parse_args()
    aws = Aws('aws-dwh.cfg')
    if args.c:
        aws.create_cluster()
    elif args.s:
        aws.get_cluster_state()
    elif args.d:
        aws.delete_cluster()
        