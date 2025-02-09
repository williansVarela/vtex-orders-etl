import logging

import pyodbc


def db_connection(
    server: str, port: str, database: str, user: str, password: str
) -> pyodbc.Connection:
    connString = f"Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:{server},{port};Database={database};UID={user};PWD={password};Encrypt=yes;TrustServerCertificate=no"
    try:
        conn = pyodbc.connect(connString)
    except Exception as e:
        logging.error(f"Failed to connect to the database. Error: {e}")
        raise e
    return conn
