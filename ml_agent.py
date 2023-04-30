import warnings
import sys
import os
import psutil
import pickle
import time
import argparse
import asyncio
import string
import redis.asyncio as redis

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

    
model_results= {
    'lineer_reg'    : 'model_results/lineer_reg.sav',    #0
    'decision_tree' : 'model_results/decision_tree.sav', #1
    'random_forest' : 'model_results/random_forest.sav', #2
    'xg_boost'      : 'model_results/xg_boost.sav',      #3
    'extra_trees'   : 'model_results/extra_trees.sav',   #4
    'ada_boost'     : 'model_results/ada_boost.sav'      #5
}
        

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

class Subscriber:
    def __init__(self):
        self.results=[]

    async def subAgent(self,node: str):
        # split argument input to match publisher's node's:
        # node-1_ActivePower --> ['node-1', 'ActivePower']
        split_str = node.split('_')  # ('NODE_NAME','VALUE_NAME')

        # Connection to redis machine
        pool = redis.ConnectionPool(host='localhost', port=6379, db=0, decode_responses=True)
        r = redis.Redis(connection_pool=pool)
        try:
            await r.ping()
        except ConnectionError as e:
            raise Exception(f"[{time.strftime('%X')}]: Cannot connect to redis server!")          

        # subscribing to specified node
        async with r.pubsub() as ps:
            # subscribe to own channel
            await ps.subscribe(split_str[0])
            # print(f"[{time.strftime('%X')}]: Subscribed to {split_str[0]}")
            print("Subscribed to {}".format(split_str[0]))
            while True:
                message = await ps.get_message(ignore_subscribe_messages=True, timeout=3)
                # if message NOT empty
                if message is not None:
                    # If incoming message, break loop and finish agent
                    if (message['data']) == "STOP":
                        print(f"[{time.strftime('%X')}]: EOF")
                        break
                    self.results = (message['channel'], message['data'])
                    time.sleep(0.001)  # be nice to the system :)
                    return self.results
                else:
                    continue

class Model:

    def __init__(self, *args, **kwargs):
        self.lineer        = LinearRegression()
        self.decision_tree = DecisionTreeRegressor() # type: ignore
        self.random_forest = RandomForestRegressor() # type: ignore
        self.xg_boost      = XGBRegressor()          # type: ignore
        self.extra_trees   = ExtraTreesRegressor()   # type: ignore
        self.ada_boost     = AdaBoostRegressor()     # type: ignore

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
        self.mae_list  = []
        self.mse_list  = []
        self.rmse_list = []
        self.r2_list   = []
        self.flag=False
        self.count=0

        self.selected_model = None
        self.selected_model_str = None

        self.splits={
            "x_train" : pd.read_csv('splits/x_train.csv'),
            "y_train" : pd.read_csv('splits/y_train.csv'),
            "x_test"  : pd.read_csv('splits/x_test.csv' ),
            "y_test"  : pd.read_csv('splits/y_test.csv' ),
            "x_val"   : pd.read_csv('splits/x_val.csv'  ),
            "y_val"   : pd.read_csv('splits/y_val.csv'  )
        } 

        
        # Run below functions when class is initialized
        if not os.path.exists('splits'):
            os.makedirs('splits')

        Model.lineer_reg(self)
        Model.decision_tree(self)
        Model.random_forest(self)
        Model.xg_boost(self)
        Model.extra_trees(self)
        Model.ada_boost(self)
        Model.model_results(self)


            
    def csvToDF(self):
        # Define the column headers
        headers = ["Execution time","Date/Time","LV ActivePower","Wind Speed","Theoretical_Power_Curve","Wind Direction"]
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

        X= self.df.drop(['LV ActivePower'] , axis = 1)
        y= self.df['LV ActivePower']

        # Split into train and test sets (80% train, 20% test)
        self.x_train , self.x_test , self.y_train , self.y_test = train_test_split(X,y, test_size=0.2, train_size=0.8, random_state=42, shuffle=False)

        # Split the remaining 20% into validation and test sets (50% each)
        self.x_test , self.x_val , self.y_test , self.y_val = train_test_split(self.x_test, self.y_test, test_size=0.5, random_state=42, shuffle=False)
        
        self.flag=True
        if not os.path.exists('splits'):
            os.makedirs('splits')
    
    def dfToCSV(self):
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
        print(f"{memory_usage()}Calculating Linear Regression...")
        if not self.flag:
            self.csvToDF() 
        self.lineer = LinearRegression()
        if not os.path.exists(model_results["lineer_reg"]): 
            self.lineer.fit(self.x_train, self.y_train)
        else:
            self.lineer=pickle.load(open(model_results["lineer_reg"], 'rb'))
        self.calculate_scores(self.lineer)
        pickle.dump(self.lineer, open(model_results["lineer_reg"], 'wb'))
        del self.lineer     
    
    @classmethod
    def decision_tree(cls, self):
        print(f"{memory_usage()}Calculating Decision Tree...")
        if not self.flag:
            self.csvToDF()
        self.decision_tree = DecisionTreeRegressor()
        if not os.path.exists(model_results["decision_tree"]): 
            self.decision_tree.fit(self.x_train, self.y_train)
        else:
            self.decision_tree=pickle.load(open(model_results["decision_tree"], 'rb'))
        self.calculate_scores(self.decision_tree)
        pickle.dump(self.decision_tree, open(model_results["decision_tree"], 'wb'))
        del self.decision_tree

    @classmethod
    def random_forest(cls, self):
        print(f"{memory_usage()}Calculating Random Forest...")
        if not self.flag:
            self.csvToDF()
        self.random_forest = RandomForestRegressor()
        if not os.path.exists(model_results["random_forest"]): 
            self.random_forest.fit(self.x_train, self.y_train)
        else:
            self.random_forest=pickle.load(open(model_results["random_forest"], 'rb'))
        self.calculate_scores(self.random_forest)
        pickle.dump(self.random_forest, open(model_results["random_forest"], 'wb'))
        del self.random_forest      
    
    @classmethod
    def xg_boost(cls, self):
        print(f"{memory_usage()}Calculating XG Boost...")
        if not self.flag:
            self.csvToDF()
        self.xg_boost = XGBRegressor()
        if not os.path.exists(model_results["xg_boost"]): 
            self.xg_boost.fit(self.x_train, self.y_train)
        else:
            self.xg_boost=pickle.load(open(model_results["xg_boost"], 'rb'))
        self.xg_boost.fit(self.x_train, self.y_train)
        self.calculate_scores(self.xg_boost)
        pickle.dump(self.xg_boost, open(model_results["xg_boost"], 'wb'))
        del self.xg_boost
    
    @classmethod
    def extra_trees(cls, self):
        print(f"{memory_usage()}Calculating Extra Trees...")
        if not self.flag:
            self.csvToDF()
        self.extra_trees = ExtraTreesRegressor()
        if not os.path.exists(model_results["extra_trees"]): 
            self.extra_trees.fit(self.x_train, self.y_train)
        else:
            self.extra_trees=pickle.load(open(model_results["extra_trees"], 'rb'))
        self.calculate_scores(self.extra_trees)
        pickle.dump(self.extra_trees, open(model_results["extra_trees"], 'wb'))
        del self.extra_trees
    
    @classmethod
    def ada_boost(cls, self):
        print(f"{memory_usage()}Calculating Ada Boost...")
        if not self.flag:
            self.csvToDF()
        self.ada_boost = AdaBoostRegressor()
        if not os.path.exists(model_results["ada_boost"]): 
            self.ada_boost.fit(self.x_train, self.y_train)
        else:
            self.ada_boost=pickle.load(open(model_results["ada_boost"], 'rb'))
        self.calculate_scores(self.ada_boost)
        pickle.dump(self.ada_boost, open(model_results["ada_boost"], 'wb'))
        del self.ada_boost
    
    def calculate_scores(self, model):
        print(f"{bcolors.OKGREEN}{model}{bcolors.ENDC}")
        y_pred=model.predict(self.splits['x_val'])
        mae = mean_absolute_error(self.splits["y_val"], y_pred)
        self.mae_list.append((self.count, mae))
        print(f'Mean absolute error: {mae:.2f}')

        # Calculate the mean squared error
        mse = mean_squared_error(self.splits["y_val"], y_pred)
        self.mse_list.append((self.count, mse))
        print(f'Mean squared error: {mse:.2f}')

        # Calculate the root mean squared error
        rmse = np.sqrt(mse)
        self.rmse_list.append((self.count, rmse))
        print(f'Root mean squared error: {rmse:.2f}')

        # Calculate the coefficient of determination (R^2)
        r2 = model.score(self.splits["x_val"], self.splits["y_val"])
        self.r2_list.append((self.count,r2))
        print(f'R^2: {r2:.2f}')
        self.count+=1

    def model_results(self):
        dataset_merged   = 'merged.csv'
        dataset_exist    = os.path.exists(os.path.join(os.getcwd(), dataset_merged))

        if dataset_exist:
            print(f'{memory_usage()} {dataset_merged} exists')
        else:
            print(f'{bcolors.FAIL}{memory_usage()} {dataset_merged} does not exist. Supply the file and try again.{bcolors.ENDC}')
            sys.exit(1)


        self.mae_list .sort(key=lambda x: x[1]) 
        self.mse_list .sort(key=lambda x: x[1]) 
        self.rmse_list.sort(key=lambda x: x[1])
        self.r2_list  .sort(key=lambda x: x[1], reverse=True)

        print(f"MAE : {self.mae_list}")
        print(f"MSE : {self.mse_list}")
        print(f"RMSE: {self.rmse_list}")
        print(f"R^2 : {self.r2_list}")

        print(f"{memory_usage()}{bcolors.WARNING} Best algorithm based on MAE  is {list(model_results.items())[self.mae_list[0][0]][0]}. {bcolors.ENDC}")
        print(f"{memory_usage()}{bcolors.WARNING} Best algorithm based on RMSE is {list(model_results.items())[self.rmse_list[0][0]][0]}.{bcolors.ENDC}")
        print(f"{memory_usage()}{bcolors.WARNING} Best algorithm based on R^2  is {list(model_results.items())[self.r2_list[0][0]][0]}.  {bcolors.ENDC}")
        
        print("")
        # Selecting the best model
        self.selected_model_str = list(model_results.items())[self.r2_list[0][0]][0]
        print(f"{memory_usage()}{bcolors.OKBLUE} {self.selected_model_str} is selected.{bcolors.ENDC}")
        self.selected_model=pickle.load(open(model_results[self.selected_model_str], 'rb'))
        del self.mae_list,self.mse_list,self.rmse_list,self.r2_list

    async def predict(self,value):
        y_pred=self.selected_model.predict(value)
        mse = mean_squared_error(model.splits["y_test"], y_pred)
        rmse = np.sqrt(mse)
        r2 = self.selected_model.score(model.splits["x_test"], model.splits["y_test"])
        print(f"{memory_usage()} Mean squared error: {mse:.2f}")
        print(f"{memory_usage()} Root mean squared error: {rmse:.2f}")
        print(f"{memory_usage()} R^2: {r2:.2f}")
        return y_pred

def memory_usage():
    # return the memory usage in MB
    process = psutil.Process(os.getpid())
    mem = process.memory_info()[0] / float(2 ** 20)
    return bcolors.HEADER + "[{0:,.2f} MB]".format(mem) + bcolors.ENDC

async def main():
    global result_list
    result_list = []
    while True:
        results = await asyncio.gather(sub.subAgent("manager"))
        if results[0] is None:
            print("EOF")
            break
        else:
            print("RESULT: "+str(results[0]))
            result_list.append(results[0])
    print("Dosya Bitti")
    print("RESULT LİST: "+str(result_list))

if __name__ == '__main__':
    if not os.path.exists('model_results'):
        os.makedirs('model_results')
    dataset_merged = 'merged.csv'

    model=Model(dataset_merged=dataset_merged)
    sub=Subscriber()
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    print(result_list[0])
    with open("incoming_x_test.csv","w",encoding="utf-8") as f:
        for i in result_list:
            i=list(i)
            f.writelines(str(i[1]))
    ############# TEST ESTIMATION BEGINS ###############
    print(f"{memory_usage()}{bcolors.OKGREEN}Estimating the test set on {model.selected_model_str}...{bcolors.ENDC}")
    #TODO
    test_df=pd.read_csv("incoming_x_test.csv")
    asyncio.run(model.predict(test_df)) 
    print(f"{memory_usage()}{bcolors.FAIL}Done.{bcolors.ENDC}")