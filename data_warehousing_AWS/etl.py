import configparser
import psycopg2
import boto3
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    print("Tables loaded.")


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    print("Data inserted in every table.")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    
    redshift = boto3.client('redshift',  region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    
    cluster_properties = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    DWH_ENDPOINT = cluster_properties['Endpoint']['Address']

    conn = psycopg2.connect(f"""host={DWH_ENDPOINT} 
                                dbname={DWH_DB} 
                                user={DWH_DB_USER}
                                password={DWH_DB_PASSWORD} 
                                port={DWH_PORT}
                            """)   
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()