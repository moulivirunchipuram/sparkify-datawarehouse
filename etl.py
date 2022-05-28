import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

# Copy data from bucket into the staging tables viz., staging_events and staging_songs

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


# Extract data from staging tables and insert into Fact and Dimension Tables        
        
def insert_tables(cur, conn):
    """Loop through insert_table_queries list
       Extract appropriate data from staging and insert them
       in to the tables
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

# Driver code which makes use of config object loaded by created_tables.py 
        
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    """Use the properties loaded in the above config object
       to connect to access the cluster and connect to the database 
    """
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    """The following function call reads data
       from the speicified source bucket and 
       copies them into the staging tables
    """
    
    load_staging_tables(cur, conn)
    
    # extract appropriate data from the staging tables and insert into Fact and Dimension tables.
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
