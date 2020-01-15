# Let's import the libraries needed to this part of the project. 
# Boto3 is the python SDK client to work with AWS resources through code.

import pandas as pd
import boto3
import json
import configparser
import time

def delete_redshift_cluster(cluster_identifier, access_key, secret_key):
    """ 
        This function instanciates de SDK Redshift Client and deletes the specified cluster after authentication. 
  
        Parameters: 
            cluster_identifier: Identificator of the Redshift cluster that is going to be deleted.
            access_key: Authentication parameter. AWS access key id of the user.
            secret_key: Authentication parameter. AWS secret access key id of the user.
    """
    
    # In the deletion of the Redshift client we must specify the Access_key and Secret_Key of the `dwhadmin` user. 
    # As Redshift is a non-global service we must fix the region in which deploy the resources: We have chosen us-west-2.
    
    redshift = boto3.client('redshift',  region_name='us-west-2', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    redshift.delete_cluster( ClusterIdentifier=cluster_identifier, SkipFinalClusterSnapshot=True)
    
    cluster_properties = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
    cluster_status = cluster_properties["ClusterStatus"]
    print (cluster_status)

    
def delete_iam_role(iam_identifier, access_key, secret_key): 
    """ 
        This function instanciate de SDK IAM Client, detaches the AmazonS3ReadOnlyAccess policy of the role 
        and deletes the specified role. 
  
        Parameters: 
            iam_identifier: Identificator of the IAM role that is going to be deleted.
            access_key: Authentication parameter. AWS access key id of the user.
            secret_key: Authentication parameter. AWS secret access key id of the user.
    """
    
    # In the creation of the IAM client we must specify the Access_key and Secret_Key of the `dwhadmin` user. 

    iam = boto3.client('iam', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    # Now we detach the AmazonS3ReadOnlyAccess policy of the role.
    
    iam.detach_role_policy(RoleName=iam_identifier, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    print(f"Policy AmazonS3ReadOnlyAccess detached of the role {iam_identifier}")
    
    # Now we delete the role.

    iam.delete_role(RoleName=iam_identifier)
    print(f"Role {iam_identifier} deleted.")

def main():
    """ 
        The main function parses the configuration, load the necessary parameters and execute the redshift and iam delete functions. 
    """
    
    # We parse the .cfg file and get the different parameters set on there. We create a dataframe with the basics ot those parameters.
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
    
    # After that, we can delete the redshift, detach the policy of the role and delete it.    
    delete_redshift_cluster(DWH_CLUSTER_IDENTIFIER, KEY, SECRET)
    delete_iam_role(DWH_IAM_ROLE_NAME, KEY, SECRET)


if __name__ == "__main__":
    main()