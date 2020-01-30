import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import boto3

# Let's read the Access_key_id and Secret_key_id of the IAM user created with S3FullAccess role policy.

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'KEY')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'SECRET')


def create_spark_session():
    """
    Function that sets the SparkSession with the dependencies necessary to run hadoop and spark on AWS.
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def list_bucket(bucket_name):
    """
    Function that list all the folders in the specified S3 bucket, using boto3 python SDK library.
    
    Params:
        bucket_name: name of the S3 bucket.
    """
    input_data = f"s3a://{bucket_name}/"
    
    conn = boto3.client('s3', 
                        region_name='us-west-2',
                        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], 
                        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    
    for key in conn.list_objects(Bucket=f"{bucket_name}")['Contents']:
        print(key['Key'])

def main():
    """
    Main function that call to the two previous functions.
    """
    spark = create_spark_session()
    list_bucket('udacity-dend')

if __name__ == "__main__":      
    main()
