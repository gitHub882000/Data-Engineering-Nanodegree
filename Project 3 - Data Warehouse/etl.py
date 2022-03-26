import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function can be called to execute the queries in ``copy_table_queries`` which
    copy the song and log data contained in S3 bucket to the staging tables.
    The song and log data are loaded from json files in S3 bucket to 2 staging tables
    ``staging_songs`` and ``staging_events``.
    
    Args:
        cur (psycopg2.connection.cursor): Cursor object for executing SQL queries.
        conn (psycopg2.connection): Connection object for commiting SQL queries.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function can be called to execute the queries in ``insert_table_queries``
    which insert the song and log data contained in staging tables to the datamart tables.
    The song and log data are loaded from 2 staging tables ``staging_songs`` and
    ``staging_events`` to the rest 5 datamart tables.
    
    Args:
        cur (psycopg2.connection.cursor): Cursor object for executing SQL queries.
        conn (psycopg2.connection): Connection object for commiting SQL queries.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Reads the Sparkify database's access information from ``dwh.cfg``.
    
    - Establishes connection with the Sparkify database and gets
    cursor to it.
    
    - Copy S3 data to staging tables.
    
    - Insert staging tables' data to datamart tables.
    
    - Finally, closes the connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DATABASE'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()