import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function executes the queries inside the copy_table_queries list.
    Those queries copy data from an S3 bucket located in a path specified in the config file.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function executes insert statement queries from the insert_table_queries list
    which populates the tables created in the create_tables.py using the data copied from the S3 bucket
    in the load_staging_tables function.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This main function reads the config file and creates a connection to a database using the parameters in the config file.
    Then executes the loading_staging_tables function and then the insert_tables function, in that order.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()