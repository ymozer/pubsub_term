import warnings
import sys
import os
import psutil
import pickle

import numpy as np
import pandas as pd

from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor 
from xgboost.sklearn import XGBRegressor
from sklearn.ensemble import ExtraTreesRegressor
from sklearn.ensemble import AdaBoostRegressor

from sklearn.model_selection import train_test_split

warnings.filterwarnings('ignore')


class bcolors:
    HEADER    = '\033[95m'
    OKBLUE    = '\033[94m'
    OKCYAN    = '\033[96m'
    OKGREEN   = '\033[92m'
    WARNING   = '\033[93m'
    FAIL      = '\033[91m'
    ENDC      = '\033[0m'
    BOLD      = '\033[1m'
    UNDERLINE = '\033[4m'

class Model:

    def __init__(self, *args, **kwargs):
        self.lineer        = LinearRegression()
        self.decision_tree = DecisionTreeRegressor()
        self.random_forest = RandomForestRegressor()
        self.xg_boost      = XGBRegressor()
        self.extra_trees   = ExtraTreesRegressor()
        self.ada_boost     = AdaBoostRegressor()

        self.csv_file = kwargs['csv_file']
        self.df       = pd.DataFrame()
        self.X        = pd.DataFrame()
        self.y        = NotImplemented
        self.x_train  = pd.DataFrame()
        self.y_train  = pd.DataFrame()
        self.x_test   = pd.DataFrame()
        self.y_test   = pd.DataFrame()
        self.x_val    = pd.DataFrame()
        self.y_val    = pd.DataFrame()
        
        Model.lineer_reg(self)
        Model.decision_tree(self)
        Model.random_forest(self)
        Model.xg_boost(self)
        Model.extra_trees(self)
        Model.ada_boost(self)
            
    def csvToDF(self):
        # Define the column headers
        headers = ['Execution time', 'Date/Time', 'LV ActivePower (kW)', 'Wind Speed (m/s)', 'Theoretical_Power_Curve (KWh)', 'Wind Direction (°)']

        # Read the CSV file into a Pandas dfFrame
        self.df = pd.read_csv(self.csv_file, names=headers, header=0, sep=';')
        # Split the date and time into separate columns
        self.df['Date/Time'] = pd.to_datetime(self.df['Date/Time'])

        self.df['YEAR']   = self.df['Date/Time'].dt.year
        self.df['MONTH']  = self.df['Date/Time'].dt.month
        self.df['DAY']    = self.df['Date/Time'].dt.day
        self.df['hour']   = self.df['Date/Time'].dt.hour
        self.df['minute'] = self.df['Date/Time'].dt.minute

        self.df = self.df.drop(columns=['Date/Time', 'Execution time'])

        X= self.df.drop(['LV ActivePower (kW)'] , axis = 1)
        y= self.df['LV ActivePower (kW)' ]

        # Split into train and test sets (80% train, 20% test)
        self.x_train , self.x_test , self.y_train , self.y_test = train_test_split(X,y, test_size=0.2, train_size=0.8, random_state=42, shuffle=False)

        # Split the remaining 20% into validation and test sets (50% each)
        self.x_test , self.x_val , self.y_test , self.y_val = train_test_split(self.x_test, self.y_test, test_size=0.5, random_state=42, shuffle=False)
    
    @classmethod
    def lineer_reg(cls, self):
        self.csvToDF() 
        self.lineer = LinearRegression()
        self.lineer.fit(self.x_train, self.y_train)
        return self.lineer
    
    @classmethod
    def decision_tree(cls, self):
        pass

    @classmethod
    def random_forest(cls, self):
        pass
    
    @classmethod
    def xg_boost(cls, self):
        pass
    
    @classmethod
    def extra_trees(cls, self):
        pass

    @classmethod
    def ada_boost(cls, self):
        pass

def memory_usage():
    # return the memory usage in MB
    process = psutil.Process(os.getpid())
    mem = process.memory_info()[0] / float(2 ** 20)
    return bcolors.HEADER + "[{0:,.2f} MB]".format(mem) + bcolors.ENDC

if __name__ == '__main__':
    csv_file   = 'merged.csv'
    model_file = 'finalized_model.sav'
    model_file_exist = os.path.exists(os.path.join(os.getcwd(), model_file))
    csv_exist        = os.path.exists(os.path.join(os.getcwd(), csv_file))

    if csv_exist:
        print(f'{memory_usage()} {csv_file} exists')
    else:
        print(f'{memory_usage()} {csv_file} does not exist. Supply the file and try again.')
        sys.exit(1)

    model=Model(csv_file=csv_file)
    # Save the model to disk
    pickle.dump(model.lineer, open(model_file, 'wb'))
    # Load the model from disk
    loaded_model = pickle.load(open(model_file, 'rb'))    
    sys.modules.pop('pickle')
    del pickle
   
    # Predict on the test set
    y_pred = loaded_model.predict(model.x_test)

    # Calculate the mean absolute error
    mae = mean_absolute_error(model.y_test, y_pred)
    print(f'Mean absolute error: {mae:.2f}')

    # Calculate the mean squared error
    mse = mean_squared_error(model.y_test, y_pred)
    print(f'Mean squared error: {mse:.2f}')

    # Calculate the root mean squared error
    rmse = np.sqrt(mse)
    print(f'Root mean squared error: {rmse:.2f}')

    # Calculate the coefficient of determination (R^2)
    r2 = loaded_model.score(model.x_test, model.y_test)
    print(f'R^2: {r2:.2f}')

