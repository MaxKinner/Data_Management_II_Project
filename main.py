# Import ETL process scripts
import extract
import transform
import load
import star_schema
import preprocessing
import model
import evaluation

# Set variables
server="localhost"
database="Fifa19"
initial_load = True


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

if initial_load == True:
    loader.create_tables(server=server, database=database)
else:
    loader.truncate_insert(server=server, database=database)

loader.insert_player_data(data=player_dim, server=server, database=database, table_to="player_dim")
loader.insert_club_data(data=club_dim, server=server, database=database, table_to="club_dim")
loader.insert_stats_data(data=stats_dim, server=server, database=database, table_to="stats_dim")
loader.insert_form_data(data=form_dim, server=server, database=database, table_to="form_dim")
loader.insert_fact_data(data=fifa_fact, server=server, database=database, table_to="fifa_fact")

### PRE-PROCESSING ###
training_data = extractor.query_data(server=server, database=database, table="fifa_fact")

preprocessor = preprocessing.PreProcessing()

X_train, x_test, Y_train, y_test = preprocessor.preprocess(
    data=training_data,
    test_size=0.25,
    train_size=0.75,
    random_state=69,
    target_variable="Value" # Possibilites: "Value", "Wage", "Release_Clause"
    )

### MODEL BUILDING ###
modeller = model.Model()

gbr_model, rfr_model, dtr_model = modeller.train_models(
    X_train,
    Y_train,
    n_estimators=1000,
    max_depth=5,
    learning_rate=0.1,
    random_state=69,
    n_jobs=6)

### EVALUATION ###
evaluator = evaluation.Evaluation()

evaluator.evaluate_models(
    gbr_model=gbr_model,
    rfr_model=rfr_model,
    dtr_model=dtr_model,
    x_test=x_test,
    y_test=y_test,
    save_plots=True
    )
