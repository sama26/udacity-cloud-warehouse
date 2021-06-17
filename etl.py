import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
      Loads the song/event data from the S3 buckets into the new staging_song and staging_event tables
      """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
      Inserts data from the staging tables into the new star schema tables defined in sql_queries.py
      """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
      Passes the config file, connects to the Redshift database and runs the 'load_staging_tables' and 'insert_tables'
      functions above.
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