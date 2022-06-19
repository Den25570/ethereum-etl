import psycopg2
from psycopg2 import OperationalError

def createConnection(dbName, dbUser, dbPassword, dbHost, dbPort):
    connection = None
    try:
        connection = psycopg2.connect(
            database=dbName,
            user=dbUser,
            password=dbPassword,
            host=dbHost,
            port=dbPort,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

def executeQuery(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")

def createDatabase(connection, dbName): 
    # Create table statement
    sqlCreateDatabase = f"create database {dbName};"
    executeQuery(connection, sqlCreateDatabase)

def pushData(connection, tableName, keys, values):
    valuesString = ""
    for valList in values:
        valuesString += f"({' ,'.join([val for val in valList])})\n"

    sqlPushData = f"""INSERT INTO {tableName} ({' ,'.join([key for key in keys])})
        VALUES {valuesString}"""
    executeQuery(connection, sqlPushData)