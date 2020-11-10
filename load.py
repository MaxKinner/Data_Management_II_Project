##############################
### LOAD                   ###
### DATA INTO THE DATABASE ###
##############################

# Import modules
import pyodbc
import sys
import pandas as pd
from tqdm import tqdm


# Class
class Load:

    def __init__(self):
        pass

    def insert_club_data(self, data, server, database, table_to):
        data = data.astype({"Club_ID": str, "Club": str, "Club_Logo": str, "Position": str, "Jersey_Number": int, "Joined": str, "Loaned_From": str, "Contract_Valid_Until": str, "Release_Clause": int})

        connection = pyodbc.connect(
        "Driver={SQL Server};"
        "Server=" + server + ";"
        "Database=" + database + ";"
        "Trusted_Connection=yes;")

        cursor = connection.cursor()

        for index,row in tqdm(data.iterrows(), total=len(data.index), ascii=True, desc="Load Club Dimension"):
            cursor.execute(
            "INSERT INTO dbo." + table_to +
            "([Club_ID],[Club],[Club_Logo],[Position],[Jersey_Number],[Joined],[Loaned_From],[Contract_Valid_Until],[Release_Clause]) VALUES (?,?,?,?,?,?,?,?,?)",
            row['Club_ID'],
            row['Club'],
            row['Club_Logo'],
            row['Position'],
            row['Jersey_Number'],
            row['Joined'],
            row['Loaned_From'],
            row['Contract_Valid_Until'],
            row['Release_Clause']
            )
            connection.commit()

        cursor.close()
        connection.close()

    def insert_player_data(self, data, server, database, table_to):
        data = data.astype({"Player_ID": str, "Age": int, "Overall": int, "Potential": int, "Special": int, "International_Reputation": int, "Skill_Moves": int, "Height": int, "Weight": int, "Name": str, "Photo": str, "Nationality": str, "Flag": str, "Value": int, "Wage": int, "Preferred_Foot": str, "Weak_Foot": int, "Work_Rate": str, "Body_Type": str, "Real_Face": str})

        connection = pyodbc.connect(
        "Driver={SQL Server};"
        "Server=" + server + ";"
        "Database=" + database + ";"
        "Trusted_Connection=yes;")

        cursor = connection.cursor()

        for index,row in tqdm(data.iterrows(), total=len(data.index), ascii=True, desc="Load Player Dimension"):
            cursor.execute(
            "INSERT INTO dbo." + table_to +
            "([Player_ID],[Age],[Overall],[Potential],[Special],[International_Reputation],[Skill_Moves],[Height],[Weight],[Name],[Photo],[Nationality],[Flag],[Value],[Wage],[Preferred_Foot],[Weak_Foot],[Work_Rate],[Body_Type],[Real_Face]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            row["Player_ID"],
            row["Age"],
            row["Overall"],
            row["Potential"],
            row["Special"],
            row["International_Reputation"],
            row["Skill_Moves"],
            row["Height"],
            row["Weight"],
            row["Name"],
            row["Photo"],
            row["Nationality"],
            row["Flag"],
            row["Value"],
            row["Wage"],
            row["Preferred_Foot"],
            row["Weak_Foot"],
            row["Work_Rate"],
            row["Body_Type"],
            row["Real_Face"]
            )
            connection.commit()

        cursor.close()
        connection.close()

    def insert_stats_data(self, data, server, database, table_to):
        data = data.astype({"Stats_ID": str, "Crossing": int, "Finishing": int, "HeadingAccuracy": int, "ShortPassing": int, "Volleys": int, "Dribbling": int, "Curve": int, "FKAccuracy": int, "LongPassing": int, "BallControl": int, "Acceleration": int, "SprintSpeed": int, "Agility": int, "Reactions": int, "Balance": int, "ShotPower": int, "Jumping": int, "Stamina": int, "Strength": int, "LongShots": int, "Aggression": int, "Interceptions": int, "Positioning": int, "Vision": int, "Penalties": int, "Composure": int, "Marking": int, "StandingTackle": int, "SlidingTackle": int, "GKDiving": int, "GKHandling": int, "GKKicking": int, "GKPositioning": int, "GKReflexes": int})

        connection = pyodbc.connect(
        "Driver={SQL Server};"
        "Server=" + server + ";"
        "Database=" + database + ";"
        "Trusted_Connection=yes;")

        cursor = connection.cursor()

        for index,row in tqdm(data.iterrows(), total=len(data.index), ascii=True, desc="Load Stats Dimension"):
            cursor.execute(
            "INSERT INTO dbo." + table_to +
            "([Stats_ID],[Crossing],[Finishing],[HeadingAccuracy],[ShortPassing],[Volleys],[Dribbling],[Curve],[FKAccuracy],[LongPassing],[BallControl],[Acceleration],[SprintSpeed],[Agility],[Reactions],[Balance],[ShotPower],[Jumping],[Stamina],[Strength],[LongShots],[Aggression],[Interceptions],[Positioning],[Vision],[Penalties],[Composure],[Marking],[StandingTackle],[SlidingTackle],[GKDiving],[GKHandling],[GKKicking],[GKPositioning],[GKReflexes]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            row["Stats_ID"],
            row["Crossing"],
            row["Finishing"],
            row["HeadingAccuracy"],
            row["ShortPassing"],
            row["Volleys"],
            row["Dribbling"],
            row["Curve"],
            row["FKAccuracy"],
            row["LongPassing"],
            row["BallControl"],
            row["Acceleration"],
            row["SprintSpeed"],
            row["Agility"],
            row["Reactions"],
            row["Balance"],
            row["ShotPower"],
            row["Jumping"],
            row["Stamina"],
            row["Strength"],
            row["LongShots"],
            row["Aggression"],
            row["Interceptions"],
            row["Positioning"],
            row["Vision"],
            row["Penalties"],
            row["Composure"],
            row["Marking"],
            row["StandingTackle"],
            row["SlidingTackle"],
            row["GKDiving"],
            row["GKHandling"],
            row["GKKicking"],
            row["GKPositioning"],
            row["GKReflexes"]
            )
            connection.commit()

        cursor.close()
        connection.close()

    def insert_form_data(self, data, server, database, table_to):
        data = data.astype({"Form_ID": str, "LS": int, "ST": int, "RS": int, "LW": int, "LF": int, "CF": int, "RF": int, "RW": int, "LAM": int, "CAM": int, "RAM": int, "LM": int, "LCM": int, "CM": int, "RCM": int, "RM": int, "LWB": int, "LDM": int, "CDM": int, "RDM": int, "RWB": int, "LB": int, "LCB": int, "CB": int, "RCB": int, "RB": int})

        connection = pyodbc.connect(
        "Driver={SQL Server};"
        "Server=" + server + ";"
        "Database=" + database + ";"
        "Trusted_Connection=yes;")

        cursor = connection.cursor()

        for index,row in tqdm(data.iterrows(), total=len(data.index), ascii=True, desc="Load Form Dimension"):
            cursor.execute(
            "INSERT INTO dbo." + table_to +
            "([Form_ID],[LS],[ST],[RS],[LW],[LF],[CF],[RF],[RW],[LAM],[CAM],[RAM],[LM],[LCM],[CM],[RCM],[RM],[LWB],[LDM],[CDM],[RDM],[RWB],[LB],[LCB],[CB],[RCB],[RB]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            row["Form_ID"],
            row["LS"],
            row["ST"],
            row["RS"],
            row["LW"],
            row["LF"],
            row["CF"],
            row["RF"],
            row["RW"],
            row["LAM"],
            row["CAM"],
            row["RAM"],
            row["LM"],
            row["LCM"],
            row["CM"],
            row["RCM"],
            row["RM"],
            row["LWB"],
            row["LDM"],
            row["CDM"],
            row["RDM"],
            row["RWB"],
            row["LB"],
            row["LCB"],
            row["CB"],
            row["RCB"],
            row["RB"]
            )
            connection.commit()

        cursor.close()
        connection.close()

    def insert_fact_data(self, data, server, database, table_to):
        data = data.astype({"Stats_ID": str, "Crossing": int, "Finishing": int, "HeadingAccuracy": int, "ShortPassing": int, "Volleys": int, "Dribbling": int, "Curve": int, "FKAccuracy": int, "LongPassing": int, "BallControl": int, "Acceleration": int, "SprintSpeed": int, "Agility": int, "Reactions": int, "Balance": int, "ShotPower": int, "Jumping": int, "Stamina": int, "Strength": int, "LongShots": int, "Aggression": int, "Interceptions": int, "Positioning": int, "Vision": int, "Penalties": int, "Composure": int, "Marking": int, "StandingTackle": int, "SlidingTackle": int, "GKDiving": int, "GKHandling": int, "GKKicking": int, "GKPositioning": int, "GKReflexes": int, "Club_ID": str, "Release_Clause": int, "Player_ID": str, "Age": int, "Overall": int, "Potential": int, "Special": int, "International_Reputation": int, "Skill_Moves": int, "Height": int, "Weight": int, "Value": int, "Wage": int, "Weak_Foot": int, "Form_ID": str, "LS": int, "ST": int, "RS": int, "LW": int, "LF": int, "CF": int, "RF": int, "RW": int, "LAM": int, "CAM": int, "RAM": int, "LM": int, "LCM": int, "CM": int, "RCM": int, "RM": int, "LWB": int, "LDM": int, "CDM": int, "RDM": int, "RWB": int, "LB": int, "LCB": int, "CB": int, "RCB": int, "RB": int})

        connection = pyodbc.connect(
        "Driver={SQL Server};"
        "Server=" + server + ";"
        "Database=" + database + ";"
        "Trusted_Connection=yes;")

        cursor = connection.cursor()

        for index,row in tqdm(data.iterrows(), total=len(data.index), ascii=True, desc="Load Fact Table"):
            cursor.execute(
            "INSERT INTO dbo." + table_to +
            "([Stats_ID],[Crossing],[Finishing],[HeadingAccuracy],[ShortPassing],[Volleys],[Dribbling],[Curve],[FKAccuracy],[LongPassing],[BallControl],[Acceleration],[SprintSpeed],[Agility],[Reactions],[Balance],[ShotPower],[Jumping],[Stamina],[Strength],[LongShots],[Aggression],[Interceptions],[Positioning],[Vision],[Penalties],[Composure],[Marking],[StandingTackle],[SlidingTackle],[GKDiving],[GKHandling],[GKKicking],[GKPositioning],[GKReflexes],[Club_ID],[Release_Clause],[Player_ID],[Age],[Overall],[Potential],[Special],[International_Reputation],[Skill_Moves],[Height],[Weight],[Value],[Wage],[Weak_Foot],[Form_ID],[LS],[ST],[RS],[LW],[LF],[CF],[RF],[RW],[LAM],[CAM],[RAM],[LM],[LCM],[CM],[RCM],[RM],[LWB],[LDM],[CDM],[RDM],[RWB],[LB],[LCB],[CB],[RCB],[RB]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            row["Stats_ID"],
            row["Crossing"],
            row["Finishing"],
            row["HeadingAccuracy"],
            row["ShortPassing"],
            row["Volleys"],
            row["Dribbling"],
            row["Curve"],
            row["FKAccuracy"],
            row["LongPassing"],
            row["BallControl"],
            row["Acceleration"],
            row["SprintSpeed"],
            row["Agility"],
            row["Reactions"],
            row["Balance"],
            row["ShotPower"],
            row["Jumping"],
            row["Stamina"],
            row["Strength"],
            row["LongShots"],
            row["Aggression"],
            row["Interceptions"],
            row["Positioning"],
            row["Vision"],
            row["Penalties"],
            row["Composure"],
            row["Marking"],
            row["StandingTackle"],
            row["SlidingTackle"],
            row["GKDiving"],
            row["GKHandling"],
            row["GKKicking"],
            row["GKPositioning"],
            row["GKReflexes"],
            row["Club_ID"],
            row["Release_Clause"],
            row["Player_ID"],
            row["Age"],
            row["Overall"],
            row["Potential"],
            row["Special"],
            row["International_Reputation"],
            row["Skill_Moves"],
            row["Height"],
            row["Weight"],
            row["Value"],
            row["Wage"],
            row["Weak_Foot"],
            row["Form_ID"],
            row["LS"],
            row["ST"],
            row["RS"],
            row["LW"],
            row["LF"],
            row["CF"],
            row["RF"],
            row["RW"],
            row["LAM"],
            row["CAM"],
            row["RAM"],
            row["LM"],
            row["LCM"],
            row["CM"],
            row["RCM"],
            row["RM"],
            row["LWB"],
            row["LDM"],
            row["CDM"],
            row["RDM"],
            row["RWB"],
            row["LB"],
            row["LCB"],
            row["CB"],
            row["RCB"],
            row["RB"]
            )
            connection.commit()

        cursor.close()
        connection.close()
    
    def create_tables(self, server, database):
        connection = pyodbc.connect(
        "Driver={SQL Server};"
        "Server=" + server + ";"
        "Database=" + database + ";"
        "Trusted_Connection=yes;")

        cursor = connection.cursor()

        cursor.execute('''
            DROP TABLE IF EXISTS stats_dim
            CREATE TABLE stats_dim
            (
            [Stats_ID] VARCHAR(50)
            ,[Crossing] int
            ,[Finishing] int
            ,[HeadingAccuracy] int
            ,[ShortPassing] int
            ,[Volleys] int
            ,[Dribbling] int
            ,[Curve] int
            ,[FKAccuracy] int
            ,[LongPassing] int
            ,[BallControl] int
            ,[Acceleration] int
            ,[SprintSpeed] int
            ,[Agility] int
            ,[Reactions] int
            ,[Balance] int
            ,[ShotPower] int
            ,[Jumping] int
            ,[Stamina] int
            ,[Strength] int
            ,[LongShots] int
            ,[Aggression] int
            ,[Interceptions] int
            ,[Positioning] int
            ,[Vision] int
            ,[Penalties] int
            ,[Composure] int
            ,[Marking] int
            ,[StandingTackle] int
            ,[SlidingTackle] int
            ,[GKDiving] int
            ,[GKHandling] int
            ,[GKKicking] int
            ,[GKPositioning] int
            ,[GKReflexes] int
            );
            DROP TABLE IF EXISTS player_dim
            CREATE TABLE player_dim
            (
            [Player_ID] VARCHAR(50)
            ,[Age] int
            ,[Overall] int
            ,[Potential] int
            ,[Special] int
            ,[International_Reputation] int
            ,[Skill_Moves] int
            ,[Height] int
            ,[Weight] int
            ,[Name] VARCHAR(50)
            ,[Photo] VARCHAR(50)
            ,[Nationality] VARCHAR(50)
            ,[Flag] VARCHAR(50)
            ,[Value] int
            ,[Wage] int
            ,[Preferred_Foot] VARCHAR(50)
            ,[Weak_Foot] int
            ,[Work_Rate] VARCHAR(50)
            ,[Body_Type] VARCHAR(50)
            ,[Real_Face] VARCHAR(50)
            );
            DROP TABLE IF EXISTS form_dim
            CREATE TABLE form_dim
            (
            [Form_ID] VARCHAR(50)
            ,[LS] int
            ,[ST] int
            ,[RS] int
            ,[LW] int
            ,[LF] int
            ,[CF] int
            ,[RF] int
            ,[RW] int
            ,[LAM] int
            ,[CAM] int
            ,[RAM] int
            ,[LM] int
            ,[LCM] int
            ,[CM] int
            ,[RCM] int
            ,[RM] int
            ,[LWB] int
            ,[LDM] int
            ,[CDM] int
            ,[RDM] int
            ,[RWB] int
            ,[LB] int
            ,[LCB] int
            ,[CB] int
            ,[RCB] int
            ,[RB] int
            );
            DROP TABLE IF EXISTS fifa_fact
            CREATE TABLE fifa_fact
            (
            [Stats_ID] VARCHAR(50)
            ,[Crossing] int
            ,[Finishing] int
            ,[HeadingAccuracy] int
            ,[ShortPassing] int
            ,[Volleys] int
            ,[Dribbling] int
            ,[Curve] int
            ,[FKAccuracy] int
            ,[LongPassing] int
            ,[BallControl] int
            ,[Acceleration] int
            ,[SprintSpeed] int
            ,[Agility] int
            ,[Reactions] int
            ,[Balance] int
            ,[ShotPower] int
            ,[Jumping] int
            ,[Stamina] int
            ,[Strength] int
            ,[LongShots] int
            ,[Aggression] int
            ,[Interceptions] int
            ,[Positioning] int
            ,[Vision] int
            ,[Penalties] int
            ,[Composure] int
            ,[Marking] int
            ,[StandingTackle] int
            ,[SlidingTackle] int
            ,[GKDiving] int
            ,[GKHandling] int
            ,[GKKicking] int
            ,[GKPositioning] int
            ,[GKReflexes] int
            ,[Club_ID] VARCHAR(50)
            ,[Release_Clause] int
            ,[Player_ID] VARCHAR(50)
            ,[Age] int
            ,[Overall] int
            ,[Potential] int
            ,[Special] int
            ,[International_Reputation] int
            ,[Skill_Moves] int
            ,[Height] int
            ,[Weight] int
            ,[Value] int
            ,[Wage] int
            ,[Weak_Foot] int
            ,[Form_ID] VARCHAR(50)
            ,[LS] int
            ,[ST] int
            ,[RS] int
            ,[LW] int
            ,[LF] int
            ,[CF] int
            ,[RF] int
            ,[RW] int
            ,[LAM] int
            ,[CAM] int
            ,[RAM] int
            ,[LM] int
            ,[LCM] int
            ,[CM] int
            ,[RCM] int
            ,[RM] int
            ,[LWB] int
            ,[LDM] int
            ,[CDM] int
            ,[RDM] int
            ,[RWB] int
            ,[LB] int
            ,[LCB] int
            ,[CB] int
            ,[RCB] int
            ,[RB] int
            );
            DROP TABLE IF EXISTS club_dim
            CREATE TABLE club_dim
            (
            [Club_ID] VARCHAR(50)
            ,[Club] VARCHAR(50)
            ,[Club_Logo] VARCHAR(50)
            ,[Position] VARCHAR(50)
            ,[Jersey_Number] int
            ,[Joined] VARCHAR(50)
            ,[Loaned_From] VARCHAR(50)
            ,[Contract_Valid_Until] VARCHAR(50)
            ,[Release_Clause] int
            )
            ''')

        connection.commit()
        cursor.close()
        connection.close()

    def truncate_insert(self, server, database):
        connection = pyodbc.connect(
        "Driver={SQL Server};"
        "Server=" + server + ";"
        "Database=" + database + ";"
        "Trusted_Connection=yes;")

        cursor = connection.cursor()
        
        cursor.execute(
            '''
                DELETE FROM dbo.club_dim;
                DELETE FROM dbo.player_dim;
                DELETE FROM dbo.stats_dim;
                DELETE FROM dbo.form_dim;
                DELETE FROM dbo.fifa_fact
            '''
        )

        connection.commit()
        cursor.close()
        connection.close()
