# Import modules

# Import ETL process scripts
import extract
import transform
import load
import star_schema

# Set variables
server="localhost"
database="Fifa19"


### EXTRACT ###
extractor = extract.Extract()

my_data = extractor.query_data(server=server, database=database, table="fifa_19")
df = my_data.copy()

### TRANSFORM ###
transformer = transform.Transform()

df = transformer.transform_data(df)

### STAR SCHEMA ###
schimera = star_schema.Star_Schema()

player_dim = schimera.apply_player_star_schema(df)
club_dim = schimera.apply_club_star_schema(df)
stats_dim = schimera.apply_stats_star_schema(df)
form_dim = schimera.apply_form_star_schema(df)
fifa_fact = schimera.create_fact(df)

### LOAD ###
loader = load.Load()

loader.create_tables(server=server, database=database)

loader.insert_club_data(data=club_dim, server=server, database=database, table_to="club_dim")
loader.insert_form_data(data=form_dim, server=server, database=database, table_to="form_dim")
loader.insert_player_data(data=player_dim, server=server, database=database, table_to="player_dim")
loader.insert_stats_data(data=stats_dim, server=server, database=database, table_to="stats_dim")
loader.insert_fact_data(data=fifa_fact, server=server, database=database, table_to="fifa_fact")

### PRE-PROCESSING ###


### MODEL BUILDING ###


