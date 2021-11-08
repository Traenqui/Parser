import mysql.connector
from mysql.connector import Error

# connect to the server
def sql_connect(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host = host_name,
            user = user_name,
            # passwd = user_password
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occured")
    return connection

# connect to a database
def database_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host = host_name,
            user = user_name,
            # passwd = user_password,
            database=db_name
        )
        print("Connection to MySQL DB " + db_name + " successful")
    except Error as e:
        print(f"The error '{e}' occured")
    return connection

def database_create(connection, database_name):
    query = 'CREATE DATABASE ' + str(database_name)
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database " + database_name + " created successfully")
    except Error as e:
        print(f"The error '{e}' occured")

def database_exists(connection , database_name):
    query = "SHOW DATABASES LIKE '" + database_name + "'"
    with connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
        if result:
            return True
        else:
            return False


#header: e.g. Product VARCHAR(255), Subproduct VARCHAR(255) ...
def table_create(connection, table_name, query):
    cursor = connection.cursor()
    command = "CREATE TABLE " + table_name + " ( " + query + ")"
    cursor.execute(command)
    print(f'Table {table_name} successfully created')


def table_exists(connection, table_name):
    query = "SHOW TABLES LIKE '" + table_name + "'"
    with connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
        if result:
            print(f'Table {table_name} exists')
            return True
        else:
            print(f'Table {table_name} does not exists')
            return False

# data: List of Tuples
def table_insert_many(connection, query, data, table_name):
    try:
        with connection.cursor() as cursor:
            cursor.executemany(query, data)
            connection.commit()
            print(cursor.rowcount, "datasets successfully inserted into " + table_name)
            return True
    except Error as e:
        print(f"Failed to insert data into the table {e}")
        return False

# delete table
def table_delete(connection, table_name):
    drop_table_query = "DROP TABLE " + str(table_name)
    with connection.cursor() as cursor:
        cursor.execute(drop_table_query)
        print(f'Table {table_name} deleted')