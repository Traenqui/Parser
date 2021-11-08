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

"""
    Idea to have a database for each quarter, with all the tables of each quarter within them, for easier debugging and updating
    
    Logic:
        - get new file
        - extract the quarter and name
        - check if database exists, if not create
        - check if table exists, if not create, if yes delete content and insert new
"""
all_paths = [
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_4LifeCZ_21Q2.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_4LifeSK_21Q2.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_Annuities_20Q4.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_Annuities_21Q1.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_Annuities_21Q2.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_AP_20Q4.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_AP_21Q1.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_AP_21Q2.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_CriticalIllness_20Q4.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_CriticalIllness_21Q1.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_CriticalIllness_21Q2.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_Fibas_20Q4.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_Fibas_21Q1.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_Fibas_21Q2.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_FPTDE_20Q4.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_FPTDE_21Q1.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_FPTDE_21Q2.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_FPTIT_20Q4.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_FPTIT_21Q1.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_FPTIT_21Q2.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_ImpairedLife_20Q4.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_ImpairedLife_21Q1.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_ImpairedLife_21Q2.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_Jool_20Q4.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_Jool_21Q1.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_Jool_21Q2.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_Pulse_20Q4.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_Pulse_21Q1.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_Pulse_21Q2.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_SinglePolicies_20Q4.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_SinglePolicies_21Q1.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_SinglePolicies_21Q2.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_Term_20Q4.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_Term_21Q1.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_Term_21Q2.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_UnitLinked_20Q4.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_UnitLinked_21Q1.csv',
    '/Users/traenqui/Projects/Valucor/Parser/test_csvfile/tblOut_UnitLinked_21Q2.csv'
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