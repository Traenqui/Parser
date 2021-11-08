import csv
from os import sep
import mysql.connector

from MySQL import *
from csv_reader import *

## script to run

# Connection
localhost = "localhost"
root="root"
password="password"
connection = sql_connect(localhost, root, password)

all_paths = [

]

# Path and variables
for path in all_paths:

    temp = path.split('_')
    quarter = temp[3]
    quarter = quarter[:4]
    table_name = temp[2]

    #queries
    header = get_header()
    data = get_data(path)
    query_table = table_query(header)
    query_insert = insert_query(table_name, header)

    # Database logic
    if not database_exists(connection, quarter):
        database_create(connection, quarter)
    # connect to the right database
    connection = database_connection(localhost, root, password, quarter)

    # Table logic
    if table_exists(connection, table_name):
        table_delete(connection, table_name)
        table_create(connection, table_name, query_table)
    else:
        table_create(connection, table_name, query_table)

    # Table insert
    table_insert_many(connection, query_insert, data, table_name)