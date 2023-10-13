import configparser
import psycopg2
from sql_queries import check_data_queries


def check_data(cur, conn):
    """Run entry counts for validation purposes."""
    for query in check_data_queries:
        print(f'Executing {query}')
        cur.execute(query)
        result = cur.fetchone()
        print(f"Result: {result[0]}\n"
              f"----------------------")
        conn.commit()


def main():
    """Run data quality check."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DWH'].values()))
    cur = conn.cursor()

    check_data(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()