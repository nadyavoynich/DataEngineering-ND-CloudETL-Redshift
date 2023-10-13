import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """load data from S3 into staging tables on Redshift."""
    for query in copy_table_queries:
        print(f'Executing {query}')
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data from staging tables to analytical tables on Redshift."""
    for query in insert_table_queries:
        print(f'Executing {query}')
        cur.execute(query)
        conn.commit()


def main():
    """Run the ETL pipeline."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DWH'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()