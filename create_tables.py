import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """drop all the tables to enable repeated testing of the application
       loops through then drop_table_queries list specified in sql_queries.py
    """
    for query in drop_table_queries:       
        cur.execute(query)
        conn.commit()

# create tables looping through list of tables defined in create_table_queries
def create_tables(cur, conn):
    """The following code loops through the create_table_queries list
       specified in sql_queries.py
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

# This is the driver code.  


def main():
    # Read configuration details from dwh.cfg file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    """Use the properties loaded in the above config object
       to connect to access the cluster and connect to the database 
    """
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
