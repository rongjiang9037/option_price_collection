import configparser
import psycopg2
import logging

from sql_queries import copy_table_queries, insert_table_queries

logging.basicConfig(level=logging.INFO)


def load_staging_tables(cur, conn):
    """
    This function copy data from S3 to staing tables.
    
    input:
    cur - database cursor variable
    conn - database connection object
    
    return - None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print("Staging table loading completed {}.".format(query))


def insert_tables(cur, conn):
    """
    This function insert data into tables from staging tables.
    
    input:
    cur - database cursor variable
    conn - database connection object
    
    return - None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    - Connect to AWS database.
    
    - Load data from S3 into staging tables.  
    
    - Transform data from staging tables into artists/songs/time tables
    
    - Finally, closes the connection. 
    
    input: None
    return: None
    """
    # read config file
    config = configparser.ConfigParser()
    config.read('/Users/margaret/OneDrive/Documents/projects/option_price_collection/config.cfg')
    
    # connect to AWS redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    logging.info("Connected to AWS redshift.")
        
    # copy data from S3 to staging tables in redshift cluster
    load_staging_tables(cur, conn)
    logging.info("Data transfer from S3 to Redshift has been completed.")
    
    # transform data from staging tables into fact & dimension tables
    insert_tables(cur, conn)
    logging.info("Data has been inserted into Redshift cluster database.")

    conn.close()


if __name__ == "__main__":
    main()
    
    