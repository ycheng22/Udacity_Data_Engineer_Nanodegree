# -*- coding: utf-8 -*-
"""
Created on Fri Apr  9 22:42:32 2021

@author: ycheng
"""

import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def create_database():
    '''
    Create database sparkifydb, connect to this database, return connection and cursor
    '''
    conn = psycopg2.connect("host=127.0.0.1 port=5432 dbname=de user=postgres password=2020")
    conn.set_session(autocommit = True)
    cur = conn.cursor()
    
    #create sparkifydb with utf-8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    
    #close connection to default database
    conn.close()
    
    #connect to sparkifydb database
    conn = psycopg2.connect("host=127.0.0.1 port=5432 dbname=sparkifydb user=postgres password=2020")
    cur = conn.cursor()
    
    return cur, conn

def drop_tables(cur, conn):
    '''Drop all tables created on the database'''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
def create_tables(cur, conn):
    '''Create tables defined on the sql_queries.py'''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
def main():
    '''
    Function to drop and re-create sparkifydb database and all related tables
    how: python create_tables.py
    '''
    cur, conn = create_database()
    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()
    
if __name__ == "__main__":
    main()
    

