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

        self.dataset_merged = kwargs['dataset_merged']
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
        self.df = pd.read_csv(self.dataset_merged, names=headers, header=0, sep=';')
        # Split the date and time into separate columns
        self.df['Date/Time'] = pd.to_datetime(self.df['Date/Time'])

        self.df['YEAR']   = self.df['Date/Time'].dt.year
        self.df['MONTH']  = self.df['Date/Time'].dt.month
        self.df['DAY']    = self.df['Date/Time'].dt.day
        self.df['hour']   = self.df['Date/Time'].dt.hour
        self.df['minute'] = self.df['Date/Time'].dt.minute

        self.df = self.df.drop(columns=['Date/Time', 'Execution time'])

        X= self.df.drop(['LV ActivePower (kW)'] , axis = 1)
        y= self.df['LV ActivePower (kW)']

        # Split into train and test sets (80% train, 20% test)
        self.x_train , self.x_test , self.y_train , self.y_test = train_test_split(X,y, test_size=0.2, train_size=0.8, random_state=42, shuffle=False)

        # Split the remaining 20% into validation and test sets (50% each)
        self.x_test , self.x_val , self.y_test , self.y_val = train_test_split(self.x_test, self.y_test, test_size=0.5, random_state=42, shuffle=False)
        
        if not os.path.exists('splits'):
            os.makedirs('splits')
    
    def csv_convert(self):
        print('Converting splitted dataframes to csv files...')
        if not os.path.exists('splits'):
            os.makedirs('splits')
        if not os.path.exists('splits/x_train.csv'):
            self.x_train.to_csv('splits/x_train.csv', index=False)
        if not os.path.exists('splits/y_train.csv'):
            self.y_train.to_csv('splits/y_train.csv', index=False)
        if not os.path.exists('splits/x_test.csv'):
            self.x_test .to_csv('splits/x_test.csv' , index=False)
        if not os.path.exists('splits/y_test.csv'):
            self.y_test .to_csv('splits/y_test.csv' , index=False)
        if not os.path.exists('splits/x_val.csv'):
            self.x_val  .to_csv('splits/x_val.csv'  , index=False)
        if not os.path.exists('splits/y_val.csv'):
            self.y_val  .to_csv('splits/y_val.csv'  , index=False)


    @classmethod
    def lineer_reg(cls, self):
        self.csvToDF() 
        self.lineer = LinearRegression()
        self.lineer.fit(self.x_train, self.y_train)
        return self.lineer
    
    @classmethod
    def decision_tree(cls, self):
        self.csvToDF()
        self.decision_tree = DecisionTreeRegressor()
        self.decision_tree.fit(self.x_train, self.y_train)
        return self.decision_tree

    @classmethod
    def random_forest(cls, self):
        self.csvToDF()
        self.random_forest = RandomForestRegressor()
        self.random_forest.fit(self.x_train, self.y_train)
        return self.random_forest
    
    @classmethod
    def xg_boost(cls, self):
        self.csvToDF()
        self.xg_boost = XGBRegressor()
        self.xg_boost.fit(self.x_train, self.y_train)
        return self.xg_boost
    
    @classmethod
    def extra_trees(cls, self):
        self.csvToDF()
        self.extra_trees = ExtraTreesRegressor()
        self.extra_trees.fit(self.x_train, self.y_train)
        return self.extra_trees
    
    @classmethod
    def ada_boost(cls, self):
        self.csvToDF()
        self.ada_boost = AdaBoostRegressor()
        self.ada_boost.fit(self.x_train, self.y_train)
        return self.ada_boost

def memory_usage():
    # return the memory usage in MB
    process = psutil.Process(os.getpid())
    mem = process.memory_info()[0] / float(2 ** 20)
    return bcolors.HEADER + "[{0:,.2f} MB]".format(mem) + bcolors.ENDC

if __name__ == '__main__':
    if not os.path.exists('model_results'):
        os.makedirs('model_results')

    dataset_merged   = 'merged.csv'
    dataset_exist    = os.path.exists(os.path.join(os.getcwd(), dataset_merged))

    if dataset_exist:
        print(f'{memory_usage()} {dataset_merged} exists')
    else:
        print(f'{bcolors.FAIL}{memory_usage()} {dataset_merged} does not exist. Supply the file and try again.{bcolors.ENDC}')
        sys.exit(1)
        
    model_results= {
        'lineer_reg'    : 'model_results/lineer_reg.sav',
        'decision_tree' : 'model_results/decision_tree.sav',
        'random_forest' : 'model_results/random_forest.sav',
        'xg_boost'      : 'model_results/xg_boost.sav',
        'extra_trees'   : 'model_results/extra_trees.sav',
        'ada_boost'     : 'model_results/ada_boost.sav'
    }
    
    loaded_model=[]
    for i in model_results:
        print(i)
        model_file_exist = os.path.exists(os.path.join(os.getcwd(), model_results[i]))
        if model_file_exist:
            print(f'{memory_usage()} {model_results[i]} exists')
            if not os.path.exists('splits'):
                print(f'{memory_usage()} splits folder does not exist.\n{bcolors.FAIL}Please delete {model_results["lineer_reg"]} and try again.{bcolors.ENDC}')
                sys.exit(1)
        else:
            print(f'{memory_usage()} {model_results[i]} does not exist. Training the model...')
            model=Model(dataset_merged=dataset_merged)
            # For some reason csv_convert is not triggering in above if statement
            print(f'{memory_usage()} splits does not exist. Creating the splits...')
            model.csv_convert()
            match i:
                case 'lineer_reg':
                    pickle.dump(model.lineer, open(model_results[i], 'wb'))
                case 'decision_tree':
                    pickle.dump(model.decision_tree, open(model_results[i], 'wb'))
                case 'random_forest':
                    pickle.dump(model.random_forest, open(model_results[i], 'wb'))
                case 'xg_boost':
                    pickle.dump(model.xg_boost, open(model_results[i], 'wb'))
                case 'extra_trees': 
                    pickle.dump(model.extra_trees, open(model_results[i], 'wb'))
                case 'ada_boost':
                    pickle.dump(model.ada_boost, open(model_results[i], 'wb'))
                case _:
                    print(f'{bcolors.FAIL}Model name is not valid. Please check the model name and try again.{bcolors.ENDC}')
    

        loaded_model.append(pickle.load(open(model_results[i], 'rb')))  
    
    sys.modules.pop('pickle')
    del pickle   

    splits={
        "x_train" : pd.read_csv('splits/x_train.csv'),
        "y_train" : pd.read_csv('splits/y_train.csv'),
        "x_test"  : pd.read_csv('splits/x_test.csv' ),
        "y_test"  : pd.read_csv('splits/y_test.csv' ),
        "x_val"   : pd.read_csv('splits/x_val.csv'  ),
        "y_val"   : pd.read_csv('splits/y_val.csv'  )
    }

    mae_list  = []
    mse_list  = []
    rmse_list = []
    r2_list   = []
    count=1
    for i in loaded_model:
        print(f"{bcolors.OKGREEN}{i}{bcolors.ENDC}")
        y_pred=i.predict(splits['x_val'])
        mae = mean_absolute_error(splits["y_val"], y_pred)
        mae_list.append((count, mae))
        print(f'Mean absolute error: {mae:.2f}')

        # Calculate the mean squared error
        mse = mean_squared_error(splits["y_val"], y_pred)
        mse_list.append((count, mse))
        print(f'Mean squared error: {mse:.2f}')

        # Calculate the root mean squared error
        rmse = np.sqrt(mse)
        rmse_list.append((count, rmse))
        print(f'Root mean squared error: {rmse:.2f}')

        # Calculate the coefficient of determination (R^2)
        r2 = i.score(splits["x_val"], splits["y_val"])
        r2_list.append((count, r2))
        print(f'R^2: {r2:.2f}')

        print(y_pred.shape)
        print()
        count+=1
    print(count)
    mae_list .sort(key=lambda x: x[1]) 
    mse_list .sort(key=lambda x: x[1]) 
    rmse_list.sort(key=lambda x: x[1])  
    rmse_list.sort(key=lambda x: x[1])
    r2_list  .sort(key=lambda x: x[1], reverse=True)

    print(f"MAE : {mae_list}")
    print(f"MSE : {mse_list}")
    print(f"RMSE: {rmse_list}")
    print(f"R^2 : {r2_list}")
    print()
    print(list(model_results.items())[5][0])
    print(f"{bcolors.WARNING}Best algorithm based on MAE  is {list(model_results.items())[mae_list[0][0]-1][0]}. {bcolors.ENDC}")
    print(f"{bcolors.WARNING}Best algorithm based on RMSE is {list(model_results.items())[rmse_list[0][0]-1][0]}.{bcolors.ENDC}")
    print(f"{bcolors.WARNING}Best algorithm based on R^2  is {list(model_results.items())[r2_list[0][0]-1][0]}.  {bcolors.ENDC}")
    selected_model = r2_list[0][0]-list(model_results.items())[r2_list[0][0]-1][0]
    print(f"{bcolors.FAIL}Done.{bcolors.ENDC}")
