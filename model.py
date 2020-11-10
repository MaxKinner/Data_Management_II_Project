##############################
### MODEL                  ###
###                        ###
##############################

# Import modules
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.tree import DecisionTreeRegressor

class Model:
    def __init__(self):
        pass

    def train_models(self, X_train, Y_train, n_estimators, max_depth, learning_rate, random_state, n_jobs):
        gbr = GradientBoostingRegressor(
            n_estimators=n_estimators,
            max_depth=max_depth,
            learning_rate=learning_rate,
            random_state=random_state
            )
        rfr = RandomForestRegressor(
            n_estimators=n_estimators,
            random_state=random_state,
            max_depth=max_depth,
            n_jobs=n_jobs
            )
        dtr = DecisionTreeRegressor(
            random_state=random_state
            )

        gbr_model = gbr.fit(X_train, Y_train)
        rfr_model = rfr.fit(X_train, Y_train)
        dtr_model = dtr.fit(X_train, Y_train)

        return gbr_model, rfr_model, dtr_model
