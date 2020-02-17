import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
        """
        This function serves to drop all the tables before creating new tables to avoid any conflicts
        """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
        """
        This function serves to create all the tables using the CRETAE queries in sql_queries.py

        """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
        """
        This main function connects to the cluster and the databse within it.
        Please noe that dwh.cfg contains necessary configurations for this function to use to connect to the cluster and database.
        Fill dwh.cfg file before running this function.

        Also, this function calls drop_table function and create_table function to create the tables within the database
        
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