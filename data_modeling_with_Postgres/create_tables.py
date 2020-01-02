import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    
    """ 
        This function creates the database sparkifydb and connect to it.
        
        Return:
            cur: Cursor of the connection.
            conn: Connection to sparkifydb using psycopg2 library.
          
    """
    
    # Connect to the default database and set autocommit.
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # Drop the sparkifydb if exists and after that it creates it.
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    conn.close()    
    
    # Connect to the sparkifydb database created.
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """ 
        This function drops tables in the list drop_table_queries, if they exist.
  
        Parameters: 
            cur: Cursor of the connection.
            conn: Connection to sparkifydb using psycopg2 library.
    """
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ 
        This function creates tables in the list create_table_queries, if they exist.
  
        Parameters: 
            cur: Cursor of the connection.
            conn: Connection to sparkifydb using psycopg2 library.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        In the main function, we call the create_database() function and use the drop_tables and create_tables,
        in that order.
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()