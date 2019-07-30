import configparser
import psycopg2
from sql_queries import copy_table_order, copy_table_queries, insert_table_order, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from the logs to the staging tables
    :param cur: The cursor of the connection
    :param conn: The connection itself
    :return:None
    """
    idx = 0
    for query in copy_table_queries:
        print("Copying data into {}...".format(copy_table_order[idx]))
        cur.execute(query)
        conn.commit()
        idx = idx + 1
        print("  [DONE]  ")


def insert_tables(cur, conn):
    """
    Translate/insert data from the staging tables to the analytical tables
    :param cur: The cursor of the connection
    :param conn: The connection itself
    :return:None
    """
    idx = 0
    for query in insert_table_queries:
        print("Inserting data into {}...".format(insert_table_order[idx]))
        cur.execute(query)
        conn.commit()
        idx = idx + 1
        print("  [DONE]  ")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()