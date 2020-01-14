# Let's import the libraries needed to this part of the project. 
# Boto3 is the python SDK client to work with AWS resources through code.

import pandas as pd
import boto3
import json
import configparser
import time

    
def create_redshift_iam_role(iam_role_name, access_key, secret_key): 
    """ 
        This function instanciate de SDK IAM Client, create a Redshift role and attaches it the AmazonS3ReadOnlyAccess policy.
  
        Parameters: 
            iam_role_name: Name of the IAM role that is going to be deleted.
            access_key: Authentication parameter. AWS access key id of the user.
            secret_key: Authentication parameter. AWS secret access key id of the user.
    """
    
    # In the creation of the IAM client we must specify the Access_key and Secret_Key of the `dwhadmin` user. 

    iam = boto3.client('iam', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    
    
    # We fix the assume the Redshift Policy document before creating the role. 
    AssumePolicyDocumentRedshift = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "redshift.amazonaws.com"
                },
        "Action": "sts:AssumeRole"
            }
        ]
    }

    # Let's create the role with the name set in the .cfg file and the previous policy.
    try:
        dwhRole = iam.create_role(RoleName=iam_role_name, 
                                  AssumeRolePolicyDocument=json.dumps(AssumePolicyDocumentRedshift),
                                  Path='/',
                                  Description='Allows Redshift clusters to call AWS services')
    
    except Exception as e:
        print(e)
        
    # Once the role is created, we can attach a predefined policy to give this role read permissions on S3 buckets.
    iam.attach_role_policy(RoleName=iam_role_name, 
                           PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess')
    
def create_redshift_cluster(clusterType, NumberOfNodes, NodeType, ClusterIdentifier, DBName, MasterUsername,
                            MasterUserPassword, Port, roleArns, access_key, secret_key):
    """ 
        This function instanciates de SDK Redshift Client and creates the specified cluster after authentication. 
  
        Parameters: 
            cluster_identifier: Identificator of the Redshift cluster that is going to be created.
            access_key: Authentication parameter. AWS access key id of the user.
            secret_key: Authentication parameter. AWS secret access key id of the user.
    """
    
    # In the creation of the Redshift client we must specify the Access_key and Secret_Key of the `dwhadmin` user. 
    # As Redshift is a non-global service we must fix the region in which deploy the resources: We have chosen us-west-2.
    
    redshift = boto3.client('redshift',  region_name='us-west-2', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    try:
        response = redshift.create_cluster(ClusterType=clusterType,
                                           NumberOfNodes=int(NumberOfNodes),
                                           NodeType=NodeType,
                                           ClusterIdentifier=ClusterIdentifier,
                                           DBName=DBName,
                                           MasterUsername=MasterUsername,
                                           MasterUserPassword=MasterUserPassword,
                                           Port=int(Port),
                                           IamRoles=[roleArns]
                                          )
           
    except Exception as e:
        print(e)
        
    cluster_properties = redshift.describe_clusters(ClusterIdentifier=ClusterIdentifier)['Clusters'][0]
    cluster_status = cluster_properties["ClusterStatus"]
    
    # Now we wait until the cluster is completely created and available.
    
    while cluster_status != "available":
        time.sleep(25.0)
        cluster_properties = redshift.describe_clusters(ClusterIdentifier=ClusterIdentifier)['Clusters'][0]
        cluster_status = cluster_properties["ClusterStatus"]
    
    print(f"Cluster Status: {cluster_status}")
    
    # Finally, we open a TCP port to access the redshift cluster open.
    
    try:
        ec2 = boto3.resource('ec2',  region_name='us-west-2', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        vpc = ec2.Vpc(id=cluster_properties['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
    
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name ,  # TODO: fill out
            CidrIp='0.0.0.0/0',  # TODO: fill out
            IpProtocol='TCP',  # TODO: fill out
            FromPort=int(Port),
            ToPort=int(Port)
        )
    except Exception as e:
        print(e)
        

        
def main():
    """ 
        The main function parses the configuration, load the necessary parameters and execute and create the AWS resources in a
        IaaC way. 
    """
    
    # We parse the .cfg file and get the different parameters set on there. We create a dataframe with the basics ot those parameters.
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")
    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
    
    # We create the iam role.
    create_redshift_iam_role(DWH_IAM_ROLE_NAME, KEY, SECRET)
    
    # We set the arn of the previously created role and create the redshift cluster
    iam = boto3.client('iam', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    DWH_ROLE_ARN = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    create_redshift_cluster(DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, 
                            DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_ROLE_ARN, KEY, SECRET)

if __name__ == "__main__":
    main()