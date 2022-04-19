import psycopg2
from sql_queries import create_statements, drop_statements


# create database
def create_database(cur, conn):
    try:
        conn.set_session(autocommit=True)
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
        cur.execute("CREATE DATABASE sparkifydb")
        print("database created")
    except psycopg2.Error as e:
        print("Error creating database")
        print(e)
    conn.commit()


# drop tables
def drop_tables(conn, cur, sql):
    for query in sql:
        try:
            cur.execute(query)
            conn.commit()
            print("drop tables")
        except psycopg2.Error as e:
            print("Error dropping tables")
            print(e)


# create tables
def create_tables(conn, cur, sql):
    for query in sql:
        try:
            cur.execute(query)
            conn.commit()
            print("tables created")
        except psycopg2.Error as e:
            print("Error creating tables")
            print(e)


# establish connection to database
def establish_connection():
    try:
        conn = psycopg2.connect(host="localhost", database="postgres", user="root", password="root")
        cur = conn.cursor()
        print("established connection")
    except psycopg2.Error as e:
        print("error establishing connection")
        print(e)

    create_database(cur, conn)

    try:
        conn = psycopg2.connect(host="localhost", database="sparkifydb", user="root", password="root")
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("error establishing new connection")
        print(e)

    return conn, cur


def main():
    # establish connection
    conn, cur = establish_connection()

    # drop tables if already created
    drop_tables(conn=conn, cur=cur, sql=drop_statements)

    # create tables in our sparkifydb database
    create_tables(conn=conn, cur=cur, sql=create_statements)


if __name__ == "__main__":
    main()
