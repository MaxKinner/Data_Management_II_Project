import pandas as pd
import sqlalchemy
import pyodbc
import urllib

server="localhost"
database="Fifa19"
table_from="fifa_19"
table_to="New_fifa_19"

# pyodbc.pooling = False

# params = urllib.parse.quote_plus("Driver={SQL Server};Server=" + server + ";Database=" + database + ";Trusted_Connection=yes;")

# engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

connection = pyodbc.connect(
    "Driver={SQL Server};"
    "Server=" + server + ";"
    "Database=" + database + ";"
    "Trusted_Connection=yes;")

data = pd.read_sql_query("SELECT * FROM Fifa19.dbo." + table_from, connection)

print(data.head(3))

