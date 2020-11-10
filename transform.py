##############################
### TRANSFORM              ###
### DATA                   ###
##############################

# Import modules
import pandas as pd

# Class
class Transform:
    def __init__(self):
        pass

    def transform_data(self, df):
        # CONSISTENCY - Uniqueness - Remove duplicates
        df.drop_duplicates(inplace=True)

        # COMPLETENESS - Remove rows with NULL values in the necessary columns: 18207 rows -> 14743 rows
        df = df[df["LS"].notna()]
        df = df[df["ST"].notna()]
        df = df[df["RS"].notna()]
        df = df[df["LW"].notna()]
        df = df[df["LF"].notna()]
        df = df[df["CF"].notna()]
        df = df[df["RF"].notna()]
        df = df[df["RW"].notna()]
        df = df[df["LAM"].notna()]
        df = df[df["CAM"].notna()]
        df = df[df["RAM"].notna()]
        df = df[df["LM"].notna()]
        df = df[df["LCM"].notna()]
        df = df[df["CM"].notna()]
        df = df[df["RCM"].notna()]
        df = df[df["RM"].notna()]
        df = df[df["LWB"].notna()]
        df = df[df["LDM"].notna()]
        df = df[df["CDM"].notna()]
        df = df[df["RDM"].notna()]
        df = df[df["RWB"].notna()]
        df = df[df["LB"].notna()]
        df = df[df["LCB"].notna()]
        df = df[df["CB"].notna()]
        df = df[df["RCB"].notna()]
        df = df[df["RB"].notna()]
        df = df[df["Crossing"].notnull()]
        df = df[df["Finishing"].notnull()]
        df = df[df["HeadingAccuracy"].notnull()]
        df = df[df["ShortPassing"].notnull()]
        df = df[df["Volleys"].notnull()]
        df = df[df["Dribbling"].notnull()]
        df = df[df["Curve"].notnull()]
        df = df[df["FKAccuracy"].notnull()]
        df = df[df["LongPassing"].notnull()]
        df = df[df["BallControl"].notnull()]
        df = df[df["Acceleration"].notnull()]
        df = df[df["SprintSpeed"].notnull()]
        df = df[df["Agility"].notnull()]
        df = df[df["Reactions"].notnull()]
        df = df[df["Balance"].notnull()]
        df = df[df["ShotPower"].notnull()]
        df = df[df["Jumping"].notnull()]
        df = df[df["Stamina"].notnull()]
        df = df[df["Strength"].notnull()]
        df = df[df["LongShots"].notnull()]
        df = df[df["Aggression"].notnull()]
        df = df[df["Interceptions"].notnull()]
        df = df[df["Positioning"].notnull()]
        df = df[df["Vision"].notnull()]
        df = df[df["Penalties"].notnull()]
        df = df[df["Composure"].notnull()]
        df = df[df["Marking"].notnull()]
        df = df[df["StandingTackle"].notnull()]
        df = df[df["SlidingTackle"].notnull()]
        df = df[df["GKDiving"].notnull()]
        df = df[df["GKHandling"].notnull()]
        df = df[df["GKKicking"].notnull()]
        df = df[df["GKPositioning"].notnull()]
        df = df[df["GKReflexes"].notnull()]
        df = df[df["Release_Clause"].notnull()]
        df = df[df["Height"].notnull()]
        df = df[df["Weight"].notnull()]
        df = df[df["Value"].notnull()]
        df = df[df["Wage"].notnull()]
        df = df[df["Special"].notnull()]
        df = df[df["Overall"].notnull()]
        df = df[df["Potential"].notnull()]
        df = df[df["Age"].notnull()]
        df = df[df["International_Reputation"].notnull()]
        df = df[df["Weak_Foot"].notnull()]
        df = df[df["Skill_Moves"].notnull()]

        # Split values into two columns
        df[["Height_Foot", "Height_Inches"]] = df.Height.str.split("'", expand=True)
        df[["LS_Base", "LS_Plus"]] = df.LS.str.split("+", expand=True)
        df[["ST_Base", "ST_Plus"]] = df.ST.str.split("+", expand=True)
        df[["RS_Base", "RS_Plus"]] = df.RS.str.split("+", expand=True)
        df[["LW_Base", "LW_Plus"]] = df.LW.str.split("+", expand=True)
        df[["LF_Base", "LF_Plus"]] = df.LF.str.split("+", expand=True)
        df[["CF_Base", "CF_Plus"]] = df.CF.str.split("+", expand=True)
        df[["RF_Base", "RF_Plus"]] = df.RF.str.split("+", expand=True)
        df[["RW_Base", "RW_Plus"]] = df.RW.str.split("+", expand=True)
        df[["LAM_Base", "LAM_Plus"]] = df.LAM.str.split("+", expand=True)
        df[["CAM_Base", "CAM_Plus"]] = df.CAM.str.split("+", expand=True)
        df[["RAM_Base", "RAM_Plus"]] = df.RAM.str.split("+", expand=True)
        df[["LM_Base", "LM_Plus"]] = df.LM.str.split("+", expand=True)
        df[["LCM_Base", "LCM_Plus"]] = df.LCM.str.split("+", expand=True)
        df[["CM_Base", "CM_Plus"]] = df.CM.str.split("+", expand=True)
        df[["RCM_Base", "RCM_Plus"]] = df.RCM.str.split("+", expand=True)
        df[["RM_Base", "RM_Plus"]] = df.RM.str.split("+", expand=True)
        df[["LWB_Base", "LWB_Plus"]] = df.LWB.str.split("+", expand=True)
        df[["LDM_Base", "LDM_Plus"]] = df.LDM.str.split("+", expand=True)
        df[["CDM_Base", "CDM_Plus"]] = df.CDM.str.split("+", expand=True)
        df[["RDM_Base", "RDM_Plus"]] = df.RDM.str.split("+", expand=True)
        df[["RWB_Base", "RWB_Plus"]] = df.RWB.str.split("+", expand=True)
        df[["LB_Base", "LB_Plus"]] = df.LB.str.split("+", expand=True)
        df[["LCB_Base", "LCB_Plus"]] = df.LCB.str.split("+", expand=True)
        df[["CB_Base", "CB_Plus"]] = df.CB.str.split("+", expand=True)
        df[["RCB_Base", "RCB_Plus"]] = df.RCB.str.split("+", expand=True)
        df[["RB_Base", "RB_Plus"]] = df.RB.str.split("+", expand=True)

        # Change the types of the newly created columns to int
        df[[
            "Height_Foot", 
            "Height_Inches",
            "LS_Base",
            "LS_Plus",
            "ST_Base",
            "ST_Plus",
            "RS_Base",
            "RS_Plus",
            "LW_Base",
            "LW_Plus",
            "LF_Base",
            "LF_Plus",
            "CF_Base",
            "CF_Plus",
            "RF_Base",
            "RF_Plus",
            "RW_Base",
            "RW_Plus",
            "LAM_Base",
            "LAM_Plus",
            "CAM_Base",
            "CAM_Plus",
            "RAM_Base",
            "RAM_Plus",
            "LM_Base",
            "LM_Plus",
            "LCM_Base",
            "LCM_Plus",
            "CM_Base",
            "CM_Plus",
            "RCM_Base",
            "RCM_Plus",
            "RM_Base",
            "RM_Plus",
            "LWB_Base",
            "LWB_Plus",
            "LDM_Base",
            "LDM_Plus",
            "CDM_Base",
            "CDM_Plus",
            "RDM_Base",
            "RDM_Plus",
            "RWB_Base",
            "RWB_Plus",
            "LB_Base",
            "LB_Plus",
            "LCB_Base",
            "LCB_Plus",
            "CB_Base",
            "CB_Plus",
            "RCB_Base",
            "RCB_Plus",
            "RB_Base",
            "RB_Plus"
        ]] = df[[
            "Height_Foot", 
            "Height_Inches",
            "LS_Base",
            "LS_Plus",
            "ST_Base",
            "ST_Plus",
            "RS_Base",
            "RS_Plus",
            "LW_Base",
            "LW_Plus",
            "LF_Base",
            "LF_Plus",
            "CF_Base",
            "CF_Plus",
            "RF_Base",
            "RF_Plus",
            "RW_Base",
            "RW_Plus",
            "LAM_Base",
            "LAM_Plus",
            "CAM_Base",
            "CAM_Plus",
            "RAM_Base",
            "RAM_Plus",
            "LM_Base",
            "LM_Plus",
            "LCM_Base",
            "LCM_Plus",
            "CM_Base",
            "CM_Plus",
            "RCM_Base",
            "RCM_Plus",
            "RM_Base",
            "RM_Plus",
            "LWB_Base",
            "LWB_Plus",
            "LDM_Base",
            "LDM_Plus",
            "CDM_Base",
            "CDM_Plus",
            "RDM_Base",
            "RDM_Plus",
            "RWB_Base",
            "RWB_Plus",
            "LB_Base",
            "LB_Plus",
            "LCB_Base",
            "LCB_Plus",
            "CB_Base",
            "CB_Plus",
            "RCB_Base",
            "RCB_Plus",
            "RB_Base",
            "RB_Plus"
        ]].apply(pd.to_numeric)

        # Add both columns togehter and overwrite the existing one
        df["LS"] = df["LS_Base"] + df["LS_Plus"]
        df["ST"] = df["ST_Base"] + df["ST_Plus"]
        df["RS"] = df["RS_Base"] + df["RS_Plus"]
        df["LW"] = df["LW_Base"] + df["LW_Plus"]
        df["LF"] = df["LF_Base"] + df["LF_Plus"]
        df["CF"] = df["CF_Base"] + df["CF_Plus"]
        df["RF"] = df["RF_Base"] + df["RF_Plus"]
        df["RW"] = df["RW_Base"] + df["RW_Plus"]
        df["LAM"] = df["LAM_Base"] + df["LAM_Plus"]
        df["CAM"] = df["CAM_Base"] + df["CAM_Plus"]
        df["RAM"] = df["RAM_Base"] + df["RAM_Plus"]
        df["LM"] = df["LM_Base"] + df["LM_Plus"]
        df["LCM"] = df["LCM_Base"] + df["LCM_Plus"]
        df["CM"] = df["CM_Base"] + df["CM_Plus"]
        df["RCM"] = df["RCM_Base"] + df["RCM_Plus"]
        df["RM"] = df["RM_Base"] + df["RM_Plus"]
        df["LWB"] = df["LWB_Base"] + df["LWB_Plus"]
        df["LDM"] = df["LDM_Base"] + df["LDM_Plus"]
        df["CDM"] = df["CDM_Base"] + df["CDM_Plus"]
        df["RDM"] = df["RDM_Base"] + df["RDM_Plus"]
        df["RWB"] = df["RWB_Base"] + df["RWB_Plus"]
        df["LB"] = df["LB_Base"] + df["LB_Plus"]
        df["LCB"] = df["LCB_Base"] + df["LCB_Plus"]
        df["CB"] = df["CB_Base"] + df["CB_Plus"]
        df["RCB"] = df["RCB_Base"] + df["RCB_Plus"]
        df["RB"] = df["RB_Base"] + df["RB_Plus"]

        ## CONFORMITY
        # - Height
        df["Height"] = (df["Height_Foot"] * 30.48) + (df["Height_Inches"] * 2.54)
        df["Height"] = df["Height"].round(decimals=1)

        # - Weight
        df["Weight"] = df["Weight"].str.replace("lbs", "")
        df[["Weight"]] = df[["Weight"]].apply(pd.to_numeric)
        df["Weight"] = df["Weight"] / 2.205
        df["Weight"] = df["Weight"].round(decimals=1)

        # - Release_Clause
        df["Release_Clause"] = df["Release_Clause"].str.replace("€", "")

        df.loc[df['Release_Clause'].str.contains("K"), 'Release_Clause_Calc'] = 1000
        df.loc[df['Release_Clause'].str.contains("M"), 'Release_Clause_Calc'] = 1000000

        df["Release_Clause"] = df["Release_Clause"].str.replace("M", "")
        df["Release_Clause"] = df["Release_Clause"].str.replace("K", "")
        df[["Release_Clause"]] = df[["Release_Clause"]].apply(pd.to_numeric)

        df["Release_Clause"] = df["Release_Clause"] * df["Release_Clause_Calc"]
        df["Release_Clause"] = df["Release_Clause"].round(decimals=0)

        # - Wage
        df["Wage"] = df["Wage"].str.replace("€", "")

        df.loc[df['Wage'].str.contains("K"), 'Wage_Calc'] = 1000
        df.loc[df['Wage'].str.contains("M"), 'Wage_Calc'] = 1000000

        df["Wage"] = df["Wage"].str.replace("M", "")
        df["Wage"] = df["Wage"].str.replace("K", "")
        df[["Wage"]] = df[["Wage"]].apply(pd.to_numeric)

        df["Wage"] = df["Wage"] * df["Wage_Calc"]
        df["Wage"] = df["Wage"].round(decimals=0)

        # - Value
        df["Value"] = df["Value"].str.replace("€", "")

        df.loc[df['Value'].str.contains("K"), 'Value_Calc'] = 1000
        df.loc[df['Value'].str.contains("M"), 'Value_Calc'] = 1000000

        df["Value"] = df["Value"].str.replace("M", "")
        df["Value"] = df["Value"].str.replace("K", "")
        df[["Value"]] = df[["Value"]].apply(pd.to_numeric)

        df["Value"] = df["Value"] * df["Value_Calc"]
        df["Value"] = df["Value"].round(decimals=0)

        return df
