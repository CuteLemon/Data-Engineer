import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, get_sample_table_queries,create_table_queries, drop_table_queries
import argparse
import pandas as pd

def drop_tables(cur, conn):
    """
    """
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    """
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def load_staging_tables(cur, conn):
    """
    load staging table from S3 to Redshift
    """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    use staging table to create fact table & dimension tables
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def get_sample_data(cur, conn):
    """
    get sample from the tables
    """
    for query in get_sample_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        num_fields = len(cur.description)
        field_names = [i[0] for i in cur.description]
        print(cur.fetchall())
        print('---------------------------')

if __name__ == "__main__":
# commandline args
    parser = argparse.ArgumentParser()
    parser.print_help()
    parser.add_argument('-pipeline',required=False, action="store_true",help='create->copy->insert->sample tables')
    parser.add_argument('-create',required=False, action="store_true",help='create table')
    parser.add_argument('-insert',required=False, action="store_true",help='insert table')
    parser.add_argument('-copy',required=False, action="store_true",help='copy table from s3 to redshift')
    parser.add_argument('-sample',required=False, action="store_true",help='get sample date')
    parser.add_argument('-drop',required=False, action="store_true",help='drop tables')
    args = parser.parse_args()

# dwh config
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

# connect database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    if args.pipeline:
       create_tables(cur,conn) 
       load_staging_tables(cur,conn)
       insert_tables(cur, conn)
       get_sample_data(cur, conn)
    elif args.create:
        create_tables(cur,conn)
    elif args.copy:
        load_staging_tables(cur,conn)
    elif args.insert:
        insert_tables(cur, conn)
    elif args.sample:
        get_sample_data(cur, conn)
    elif args.drop:
        drop_tables(cur,conn)

    conn.close()