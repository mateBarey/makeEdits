import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import time
from datetime import datetime
from datetime import timedelta
import xgboost as xgb 
import torch
import torch.nn as nn
from sklearn.ensemble import *
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

def xgboostpred(df):
    minmax = MinMaxScaler().fit(df.iloc[:, 7].values.reshape((-1,1)))
    close_normalize = minmax.transform(df.iloc[:, 7].values.reshape((-1,1))).reshape((-1))
    date_ori = pd.to_datetime(df.iloc[:, 0]).tolist()

    class Encoder(nn.Module):
        def __init__(self, input_size, dimension=2, hidden_layer=256, learning_rate=0.01, epoch=20):
            super(Encoder, self).__init__()

            self.fc1 = nn.Linear(input_size, hidden_layer)
            nn.init.normal_(self.fc1.weight, mean=0, std=0.01)
            nn.init.normal_(self.fc1.bias, mean=0, std=0.01)

            self.fc2 = nn.Linear(hidden_layer, dimension)
            nn.init.normal_(self.fc2.weight, mean=0, std=0.01)
            nn.init.normal_(self.fc2.bias, mean=0, std=0.01)

            self.fc3 = nn.Linear(dimension, hidden_layer)
            nn.init.normal_(self.fc3.weight, mean=0, std=0.01)
            nn.init.normal_(self.fc3.bias, mean=0, std=0.01)

            self.fc4 = nn.Linear(hidden_layer, input_size)
            nn.init.normal_(self.fc4.weight, mean=0, std=0.01)
            nn.init.normal_(self.fc4.bias, mean=0, std=0.01)

            self.activation = nn.Sigmoid()
            self.loss = nn.MSELoss()
            self.optimizer = torch.optim.RMSprop(self.parameters(), lr=learning_rate)

        def forward(self, X):
            first_layer_encoder = self.activation(self.fc1(X))
            second_layer_encoder = self.activation(self.fc2(first_layer_encoder))
            first_layer_decoder = self.activation(self.fc3(second_layer_encoder))
            second_layer_decoder = self.activation(self.fc4(first_layer_decoder))
            return second_layer_encoder, second_layer_decoder

        def fit(self, input_data, epoch=20):
            for i in range(epoch):
                last_time = time.time()
                _, output = self.forward(input_data)
                loss = self.loss(input_data, output)
                self.optimizer.zero_grad()
                loss.backward()
                self.optimizer.step()
                #if (i + 1) % 10 == 0:
                    #print('epoch:', i + 1, 'loss:', loss.item(), 'time:', time.time() - last_time)

        def encode(self, input_data):
            with torch.no_grad():
                second_layer_encoder, _ = self.forward(input_data)
            return second_layer_encoder
        
    input_data = torch.tensor(close_normalize.reshape((-1, 1)), dtype=torch.float32)
    encoder=Encoder(input_size=input_data.size(1),dimension=32, learning_rate=0.01, hidden_layer=128, epoch=100)
    encoder.fit(input_data,epoch=100)
    thought_vector = encoder.encode(input_data)
    ada = AdaBoostRegressor(n_estimators=500, learning_rate=0.1)
    bagging = BaggingRegressor(n_estimators=500)
    et = ExtraTreesRegressor(n_estimators=500)
    gb = GradientBoostingRegressor(n_estimators=500, learning_rate=0.1)
    rf = RandomForestRegressor(n_estimators=500)
    ada.fit(thought_vector[:-1, :], close_normalize[1:])
    bagging.fit(thought_vector[:-1, :], close_normalize[1:])
    et.fit(thought_vector[:-1, :], close_normalize[1:])
    gb.fit(thought_vector[:-1, :], close_normalize[1:])
    rf.fit(thought_vector[:-1, :], close_normalize[1:])
    ada_pred=ada.predict(thought_vector)
    bagging_pred=bagging.predict(thought_vector)
    et_pred=et.predict(thought_vector)
    gb_pred=gb.predict(thought_vector)
    rf_pred=rf.predict(thought_vector)
    ada_actual = np.hstack([close_normalize[0],ada_pred[:-1]])
    bagging_actual = np.hstack([close_normalize[0],bagging_pred[:-1]])
    et_actual = np.hstack([close_normalize[0],et_pred[:-1]])
    gb_actual = np.hstack([close_normalize[0],gb_pred[:-1]])
    rf_actual = np.hstack([close_normalize[0],rf_pred[:-1]])
    stack_predict = np.vstack([ada_actual,bagging_actual,et_actual,gb_actual,rf_actual,close_normalize]).T
    corr_df = pd.DataFrame(stack_predict)
    params_xgd = {
    'max_depth': 7,
    'objective': 'reg:logistic',
    'learning_rate': 0.05,
    'n_estimators': 10000,
    'early_stopping_rounds':10,
    'eval_metric':'rmse'
    }
    train_Y = close_normalize[1:]
    clf = xgb.XGBRegressor(**params_xgd)
    clf.fit(stack_predict[:-1,:],train_Y, eval_set=[(stack_predict[:-1,:],train_Y)])
    xgb_pred = clf.predict(stack_predict)
    xgb_actual = np.hstack([close_normalize[0],xgb_pred[:-1]])
    date_original=pd.Series(date_ori).dt.strftime(date_format='%Y-%m-%d').tolist()
    def reverse_close(array):
        return minmax.inverse_transform(array.reshape((-1,1))).reshape((-1))
    ada_list = ada_pred.tolist()
    bagging_list = bagging_pred.tolist()
    et_list = et_pred.tolist()
    gb_list = gb_pred.tolist()
    rf_list = rf_pred.tolist()
    xgb_list = xgb_pred.tolist()
    def predict(count, history = 5):
        for i in range(count):
            roll = np.array(xgb_list[-history:])
            input_data = torch.tensor(roll.reshape((-1, 1)), dtype=torch.float32)
            thought_vector = encoder.encode(input_data)
            ada_pred=ada.predict(thought_vector)
            bagging_pred=bagging.predict(thought_vector)
            et_pred=et.predict(thought_vector)
            gb_pred=gb.predict(thought_vector)
            rf_pred=rf.predict(thought_vector)
            ada_list.append(ada_pred[-1])
            bagging_list.append(bagging_pred[-1])
            et_list.append(et_pred[-1])
            gb_list.append(gb_pred[-1])
            rf_list.append(rf_pred[-1])
            ada_actual = np.hstack([xgb_list[-history],ada_pred[:-1]])
            bagging_actual = np.hstack([xgb_list[-history],bagging_pred[:-1]])
            et_actual = np.hstack([xgb_list[-history],et_pred[:-1]])
            gb_actual = np.hstack([xgb_list[-history],gb_pred[:-1]])
            rf_actual = np.hstack([xgb_list[-history],rf_pred[:-1]])
            stack_predict = np.vstack([ada_actual,bagging_actual,et_actual,gb_actual,rf_actual,xgb_list[-history:]]).T
            xgb_pred = clf.predict(stack_predict)
            xgb_list.append(xgb_pred[-1])
            date_ori.append(date_ori[-1]+timedelta(days=1))
    #changed for backtester history=30 to 20 will change back to 30
    predict(2,history=20)
    return reverse_close(np.array(xgb_list))[-1]