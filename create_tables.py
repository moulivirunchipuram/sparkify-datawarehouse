import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# drop all the tables to enable repeated testing of the application
# loops through the list of tables defined in drop_table_queries
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

# create tables looping through list of tables defined in create_table_queries
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

# This is the driver code.  
# Read configuration details from dwh.cfg file

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
