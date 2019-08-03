import configparser
import psycopg2
from sql_queries import create_table_queries, create_schema_queries, drop_table_queries, drop_schema_queries

def drop_tables(cur, conn):
    """
    Arguments:
    cur - cursor object for postgresql
    conn - connection object
    
    Description: Executes all the queries in drop_table_queries list, which are basically SQL scripts for dropping table objects
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
def drop_schemas(cur, conn):
    """
    Arguments:
    cur - cursor object for postgresql
    conn - connection object
    
    Description: Executes all the queries in drop_schema_queries list, which are basically SQL scripts for dropping schema objects
    """
    for query in drop_schema_queries:
        cur.execute(query)
        conn.commit()
        
def create_schemas(cur, conn):
    """
    Arguments:
    cur - cursor object for postgresql
    conn - connection object
    
    Description: Executes all the queries in create_schema_queries list, which are basically SQL scripts for creating schema objects
    """
    for query in create_schema_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    Arguments:
    cur - cursor object for postgresql
    conn - connection object
    
    Description: Executes all the queries in create_table_queries list, which are basically SQL scripts for creating table objects
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Here we basically get the config settings using config parser. Use those settings to create a connection object
    Use connection object to create a cursor object. Use the cursor and connection arguments to call drop_schema, drop_tables, 
    create_schema and create_tables function
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    drop_tables(cur, conn)
    drop_schemas(cur, conn)
    create_schemas(cur, conn)
    create_tables(cur, conn)
    conn.close()

if __name__ == "__main__":
    main()