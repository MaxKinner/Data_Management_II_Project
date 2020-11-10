from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import matplotlib.pyplot as plt

class Evaluation:
    def __init__(self):
        pass

    def evaluate_models(self, gbr_model, rfr_model, dtr_model, x_test, y_test, save_plots):
        print("Evaluate models started ################")
        y_predicted_gbr = gbr_model.predict(x_test)
        y_predicted_rfr = rfr_model.predict(x_test)
        y_predicted_dtr = dtr_model.predict(x_test)

        y_predicted_total = [y_predicted_gbr, y_predicted_rfr, y_predicted_dtr]

        predict_counter = 0

        for y_predicted in y_predicted_total:

            if predict_counter == 0:
                print("### Gradient Boosting Model ###")
            elif predict_counter == 1:
                print("### Random Forest Model ###")
            else:
                print("### Decision Tree Model ###")

            fig, ax = plt.subplots()
            ax.scatter(y_predicted, y_test, edgecolors=(0, 0, 1))
            ax.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--', lw=3)
            ax.set_xlabel('Predicted')
            ax.set_ylabel('Actual')
            plt.show()

            mae = mean_absolute_error(y_test, y_predicted) # Robust to outliers
            mse = mean_squared_error(y_test, y_predicted) # Not robust to outliers
            r2 = r2_score(y_test, y_predicted) # Not robust to outliers

            print("--------------------------------------")
            print('MAE is {}'.format(mae))
            print('MSE is {}'.format(mse))
            print('R2 score is {}'.format(r2))

            if save_plots == True:
                if predict_counter == 0:
                    plt.savefig("Gradient_Boosting.png", bbox_inches='tight', dpi=199)
                elif predict_counter == 1:
                    plt.savefig("Random_Forest.png", bbox_inches='tight', dpi=199)
                else:
                    plt.savefig("Decision_Tree.png", bbox_inches='tight', dpi=199)

            predict_counter += 1

        print("Evaluate models finished ################")