import configparser
import psycopg2
import logging

from sql_queries import create_table_queries, drop_table_queries

logging.basicConfig(level=logging.INFO)


def drop_tables(cur, conn):
    """
    Drop each table with queries in `drop_table_queries` list.
    
    input:
    cur: cursor variable
    conn: database connection object
    
    return: None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
def create_tables(cur, conn):
    """
    - Create tables with queries in 'create_table_queries' list.

    input: None
    return: None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
        
def main():
    """
    - Connect to AWS redshift database.
    
    - Drop all tables.  
    
    - Create all tables needed. 
    
    - Finally, close the connection. 
    
    input: None
    return: None
    """
    # read config file
    config = configparser.ConfigParser()
    config.read('/Users/margaret/OneDrive/Documents/projects/option_price_collection/config.cfg')
    
    # connect aws redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    logging.info("Connected to AWS redshift.")

    # drop fact, dimension, staging table if they exist
    drop_tables(cur, conn)
    logging.info("Dropped fact, dimension, staging tables if they exist.")
    
    # create fact, dimension, staging table
    create_tables(cur, conn)
    logging.info("Created fact, dimension, staging tables if they don't exist.")

    conn.close()

if __name__ == "__main__":
    main()

