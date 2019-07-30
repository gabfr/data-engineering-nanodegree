import configparser
import psycopg2
from sql_queries import analytical_queries, analytical_query_titles


def run_analytical_queries(cur):
    """
    Runs all analytical queries written in the sql_queries script
    :param cur:
    :return:
    """
    idx = 0
    for query in analytical_queries:
        print("{}... ".format(analytical_query_titles[idx]))
        row = cur.execute(query)
        print(row.total)
        idx = idx + 1
        print("  [DONE]  ")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    run_analytical_queries(cur)

    conn.close()


if __name__ == "__main__":
    main()