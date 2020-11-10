##############################
### PRE-PROCESSING         ###
###                        ###
##############################

# Import modules
from sklearn.model_selection import train_test_split
import pandas as pd


class PreProcessing:
    def __init__(self):
        pass

    def preprocess(self, data, test_size, train_size, random_state, target_variable):
        # Drop not used columns
        data.drop(columns=["Stats_ID", "Form_ID", "Player_ID", "Club_ID"], inplace=True)
        
        predictors = data.copy()
        predictors.drop(columns=[target_variable], inplace=True)
        
        dependents = data.copy()
        dependents = dependents.filter(items=[target_variable])

        # Divide into test and train data
        X_train, x_test, Y_train, y_test = train_test_split(predictors, dependents, test_size=test_size, train_size=train_size, random_state=random_state)

        return X_train, x_test, Y_train, y_test
