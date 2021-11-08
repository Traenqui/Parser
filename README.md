
## Notes
Create venv
```$ python3 -m venv parser```

activte venv
```$ source parser/bin/activte```

create requirements.txt
```$ pip freeze > requirements.txt```

deactivate venv
``` $ deactivate```

```pip install mysql-connector-python```

Don't forget to start sql server
```mysql.server start```

Get docker ready
```docker-compose up```

## Idea
To have a database for each quarter, with all the tables for each product within

### Logic
- get a path to a new csv file
- extract the quarter as database name
- extract the productname as table_name
- check if database exists
    - if not create
- check if table exists
    - if yes, drop table and create new
    - if no create new

## Specs
### Input
- Input is a excel or csv file

### Output
- (Debug) Consol-log (1. String output like SQL import-statment) 
- maybe xxx% and how many are alredy done

## Goal

The goal of this parser is to

1. extract the frist line as a header array
2. set the header as colums for MySQL import
3. parse each line seperately and import into MySQL DB

## How the functions work
The sql_connect function, creates a connection to the SQL Server 
```def sql_connect(host_name, user_name, user_password)```

The database_connection function, redefines the connectionobject to connect to the SQL Server and one specified database
```def database_connection(host_name, user_name, user_password, db_name)```

## ToDo
- [] check delimiter csv
- [x] create truncate table function or delete table
- [x] setup the logic behind if table/database exists and what to do
- [] Add function to set 'PRIMARY KEY'
- [] Add UNION function to get output

### ToDo for later Updates
- Implement a function to check S2-Values
- Implement a function to check StatRes-Values
- Implement a function to check 'Active'-policies

### ToDo for generating the data
- Find a way to automate most of the process
    - maybe automatic Access functions into Excel 
    - Function to collect S2 and Stat-Res as far as possible

