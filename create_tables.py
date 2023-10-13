import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop tables if the tables already exist."""
    for query in drop_table_queries:
        print(f'Executing {query}')
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create staging and analytical tables."""
    for query in create_table_queries:
        print(f'Executing {query}')
        cur.execute(query)
        conn.commit()


def main():
    """Run table creation on Redshift."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DWH'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()