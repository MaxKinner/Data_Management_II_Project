##############################
### EXTRACT                ###
### DATA FROM THE DATABASE ###
##############################

# Import modules
import pandas as pd
import pyodbc
#import pymssql


# Class
class Extract:
    """The Extract class provides the function for the database connection and the data extraction
    """    

    def __init__(self):
        pass

    def query_data(self, server, database, table):
        """Connects to the database and queries the data

        :param server: The server where the database is running on
        :type server: String
        :param database: The database which should be accessed
        :type database: String
        :param table: The table which should be accessed
        :type table: String
        :return: Returns the whole given table
        :rtype: Pandas Dataframe
        """

        connection = pyodbc.connect(
            "Driver={SQL Server};"
            "Server=" + server + ";"
            "Database=" + database + ";"
            "Trusted_Connection=yes;")

        query = pd.read_sql_query("SELECT * FROM Fifa19.dbo." + table, connection)

        df = pd.DataFrame(query)
        return df
