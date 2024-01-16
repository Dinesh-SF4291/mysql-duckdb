from typing import List

import dlt
import duckdb
import pandas as pd
import mysql.connector
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.common import pendulum

from sql_database import sql_database, sql_table

def sin():
    connection_string = ConnectionStringCredentials("mssql+pyodbc://sa:Dinesh123@SYNCLAPN-38095:1433/test?driver=ODBC+Driver+17+for+SQL+Server")

    pipeline = dlt.pipeline(
        pipeline_name="jagan", destination='duckdb', dataset_name="jagan_data"
    )

    source=sql_database(connection_string).with_resources("sample")
    info=pipeline.run(source,write_disposition="replace",table_name="jagan")
    print(info)

    duckdb_conn = duckdb.connect(database='jagan.duckdb')

    # Execute a query and fetch results into a pandas DataFrame
    duckdb_query = "SELECT * FROM jagan_data.jagan;"
    data_result = duckdb_conn.execute(duckdb_query)
    check_table_query = f"SELECT name FROM sqlite_master WHERE type IN ('table', 'view') AND name = 'jagan_data.jagan'"

    table_exists = duckdb_conn.execute(check_table_query).fetchall()
    with pipeline.sql_client() as client:
        with client.execute_query(
            f"SELECT name FROM sqlite_master WHERE type IN ('table', 'view') AND name = 'jagan_data.jagan'"
        ) as table:
            duckdb_df = table.df()

    # Disconnect from DuckDB
    duckdb_conn.close()

    # Connect to MySQL
    mysql_conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Dinesh123',
        database='test'
    )

    # Create a cursor for MySQL
    mysql_cursor = mysql_conn.cursor()

    # Create the MySQL table if it doesn't exist
    mysql_table_name = 'testing'
    create_table_query = """
        CREATE TABLE IF NOT EXISTS {table} (
            {columns}
        );
    """.format(table=mysql_table_name, columns=', '.join(f"{col} VARCHAR(255)" for col in duckdb_df.columns))
    mysql_cursor.execute(create_table_query)
    mysql_conn.commit()

    # Insert data into the MySQL table
    insert_query = "INSERT INTO {table} ({columns}) VALUES ({values})"
    for _, row in duckdb_df.iterrows():
        values = ', '.join("'" + str(value).replace("'", "''") + "'" for value in row)
        mysql_cursor.execute(insert_query.format(table=mysql_table_name, columns=', '.join(row.index), values=values))

    # Commit the changes to the MySQL database
    mysql_conn.commit()

    # Disconnect from MySQL
    mysql_conn.close()

if __name__ == "__main__":
    sin()



