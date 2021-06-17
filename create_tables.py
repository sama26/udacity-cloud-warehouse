import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
      Drops any tables created by this script, as defined in the sql_queries.py file
      """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
      Creates all tables defined in the sql_queries.py file
      """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
      Reads in 'dwh.cfg' config file, connects to Redshift database and then drops and re-creates the tables defined in
      the sql_queries.py file

      When finished it closes the connection to Redshift
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
