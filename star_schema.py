##############################
### STAR SCHEMA            ###
###                        ###
##############################

# Import libraries
import pandas as pd

# WG ###################
server="localhost"
database="Fifa19"
table_from="fifa_19"
import extract
import transform
extractor = extract.Extract()
transformer = transform.Transform()

df = extractor.query_data(server=server, database=database, table=table_from)
df = transformer.transform_data(df)

#########################

class Star_Schema:
    def __init__(self):
        pass

    def apply_stats_star_schema(self, df):
        # Stats Dimension
        stats = df.copy()
        stats = df.filter(items=["ID", "Crossing", "Finishing", "HeadingAccuracy", "ShortPassing", "Volleys", "Dribbling", "Curve", "FKAccuracy", "LongPassing", "BallControl", "Acceleration", "SprintSpeed", "Agility", "Reactions", "Balance", "ShotPower", "Jumping", "Stamina", "Strength", "LongShots", "Aggression", "Interceptions", "Positioning", "Vision", "Penalties", "Composure", "Marking", "StandingTackle", "SlidingTackle", "GKDiving", "GKHandling", "GKKicking", "GKPositioning", "GKReflexes"])
        stats.rename(columns={"ID" : "Stats_ID"}, inplace=True)
        return stats

    def apply_player_star_schema(self, df):
        # Player Dimension
        player = df.copy()
        player = df.filter(items=["ID", "Age", "Overall", "Potential", "Special", "International_Reputation", "Skill_Moves", "Height", "Weight", "Name", "Photo", "Nationality", "Flag", "Value", "Wage", "Preferred_Foot", "Weak_Foot", "Work_Rate", "Body_Type", "Real_Face"])
        player.rename(columns={"ID" : "Player_ID"}, inplace=True)
        return player

    def apply_form_star_schema(self, df):
        # Form Dimension
        form = df.copy()
        form = df.filter(items=["ID", "LS", "ST", "RS", "LW", "LF", "CF", "RF", "RW", "LAM", "CAM", "RAM", "LM", "LCM", "CM", "RCM", "RM", "LWB", "LDM", "CDM", "RDM", "RWB", "LB", "LCB", "CB", "RCB", "RB"])
        form.rename(columns={"ID" : "Form_ID"}, inplace=True)
        return form

    def apply_club_star_schema(self, df):
        # Club Dimension
        club = df.copy()
        club = df.filter(items=["ID", "Club", "Club_Logo", "Position", "Jersey_Number", "Joined", "Loaned_From", "Contract_Valid_Until", "Release_Clause"])
        club.rename(columns={"ID" : "Club_ID"}, inplace=True)
        return club

    def create_fact(self, df):
        # Fact Table
        fact = df.copy()
        fact = df.filter(items=[
            "Stats_ID", "Crossing", "Finishing", "HeadingAccuracy", "ShortPassing", "Volleys", "Dribbling", "Curve", "FKAccuracy", "LongPassing", "BallControl", "Acceleration", "SprintSpeed", "Agility", "Reactions", "Balance", "ShotPower", "Jumping", "Stamina", "Strength", "LongShots", "Aggression", "Interceptions", "Positioning", "Vision", "Penalties", "Composure", "Marking", "StandingTackle", "SlidingTackle", "GKDiving", "GKHandling", "GKKicking", "GKPositioning", "GKReflexes",
            "Club_ID", "Release_Clause",
            "Player_ID", "Age", "Overall", "Potential", "Special", "International_Reputation", "Skill_Moves", "Height", "Weight", "Value", "Wage", "Weak_Foot",
            "Form_ID", "LS", "ST", "RS", "LW", "LF", "CF", "RF", "RW", "LAM", "CAM", "RAM", "LM", "LCM", "CM", "RCM", "RM", "LWB", "LDM", "CDM", "RDM", "RWB", "LB", "LCB", "CB", "RCB", "RB"
            ])
        return fact
