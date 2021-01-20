import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function drops all the tables inside de drop_table_queries list.
    If for some reason the project gets bigger and needs more tables to be dropped at a different time,
    the drop_table_queries can be changed for a function parameter.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function executes create statements inside create_table_queries list brought from sql_queries.
    The execution order inside the list is in sql_queries.py.
    If for some reason the project gets bigger and needs more tables at a different time,
    the create_table_queries can be changed for a function parameter.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function reads the config file dwh and creates a connection with the DB using the credentials inside the file.
    Then executes drop_tables function and create_tables function.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()