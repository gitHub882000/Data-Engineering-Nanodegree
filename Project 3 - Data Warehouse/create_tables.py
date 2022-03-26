import configparser
import psycopg2
from sql_queries import create_schema_queries, drop_schema_queries, create_table_queries


def drop_schemas(cur, conn):
    """
    Drops each schema using the queries in ``drop_schema_queries`` list.
    """
    for query in drop_schema_queries:
        cur.execute(query)
        conn.commit()


def create_schemas(cur, conn):
    """
    Creates each schema using the queries in ``create_schema_queries`` list. 
    """
    for query in create_schema_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in ``create_table_queries`` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Reads the Sparkify database's access information from ``dwh.cfg``.
    
    - Establishes connection with the Sparkify database and gets
    cursor to it.
    
    - Drops all schemas, note that dropping schemas alone is sufficient
    to clean the database.
    
    - Creates all schemas and tables needed.
    
    - Finally, closes the connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DATABASE'].values()))
    cur = conn.cursor()

    drop_schemas(cur, conn)
    create_schemas(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()