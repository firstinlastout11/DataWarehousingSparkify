import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
        """
        This function serves to load all the information from the json objects in the Sparkify S3 to Staging tables.
        There are two staging tables : staging_events and staging_songs.

        """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
        """
        This function serves to insert the data into the right tables from staging tables.
        It utilizes insert_table_queries in sql_queries.py.

        """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
        """
        This main function connects to the database utilizing the configurations in dwh.cfg file.
        Please fill the file before running this file.
        Then, it calls load_staging_tables function and insert_table function to execute the ETL process.
        
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