import numpy as np
import pandas as pd
from datetime import datetime
import talib as ta
from asyncio import * 
import time 
#from LoggerC import Logger
from model_mod import Model 
#logger = Logger(r'C:\Users\grc\Downloads\Logs Decision')
    
def get_indicators(data):
    # Get MACD
    data["macd"], data["macd_signal"], data["macd_hist"] = ta.MACD(data['Close'])
    # Get MA10 and MA30
    data["ma10"] = ta.MA(data["Close"], timeperiod=10)
    data["ma30"] = ta.MA(data["Close"], timeperiod=30)
    # Get RSI
    data["rsi"] = ta.RSI(data["Close"], timeperiod=14)
    #OBV
    data["Obv"] = ta.OBV(data["Close"],data["Volume"])
    return data
    
window_size = 20
skip = 1
layer_size = 500
output_size = 3

def softmax(z):
    assert len(z.shape) == 2
    s = np.max(z, axis=1)
    s = s[:, np.newaxis]
    e_x = np.exp(z - s)
    div = np.sum(e_x, axis=1)
    div = div[:, np.newaxis]
    return e_x / div

def get_state(parameters, t, window_size = 20):
    outside = []
    d = t - window_size + 1
    for parameter in parameters:
        block = (
            parameter[d : t + 1]
            if d >= 0
            else -d * [parameter[0]] + parameter[0 : t + 1]
        )
        res = []
        for i in range(window_size - 1):
            res.append(block[i + 1] - block[i])
        for i in range(1, window_size, 1):
            res.append(block[i] - block[0])
        outside.append(res)
    return np.array(outside).reshape((1, -1))

class Deep_Evolution_Strategy:

    inputs = None

    def __init__(
        self, weights, reward_function, population_size, sigma, learning_rate
    ):
        self.weights = weights
        self.reward_function = reward_function
        self.population_size = population_size
        self.sigma = sigma
        self.learning_rate = learning_rate

    def _get_weight_from_population(self, weights, population):
        weights_population = []
        for index, i in enumerate(population):
            jittered = self.sigma * i
            weights_population.append(weights[index] + jittered)
        return weights_population

    def get_weights(self):
        return self.weights
    
    def train(self, epoch = 100, print_every = 1):
        lasttime = time.time()
        for i in range(epoch):
            population = []
            rewards = np.zeros(self.population_size)
            for k in range(self.population_size):
                x = []
                for w in self.weights:
                    x.append(np.random.randn(*w.shape))
                population.append(x)
            for k in range(self.population_size):
                weights_population = self._get_weight_from_population(
                    self.weights, population[k]
                )
                rewards[k] = self.reward_function(weights_population)
            rewards = (rewards - np.mean(rewards)) / (np.std(rewards) + 1e-7)
            for index, w in enumerate(self.weights):
                A = np.array([p[index] for p in population])
                self.weights[index] = (
                    w
                    + self.learning_rate
                    / (self.population_size * self.sigma)
                    * np.dot(A.T, rewards).T
                )
            if (i + 1) % print_every == 0:
                print(
                    'iter %d. reward: %f'
                    % (i + 1, self.reward_function(self.weights))
                )
        print('time taken to train:', time.time() - lasttime, 'seconds')
        


class Agent:

    POPULATION_SIZE = 15
    SIGMA = 0.1
    LEARNING_RATE = 0.03

    def __init__(self, model, timeseries, skip, initial_money, real_trend, minmax,max_buy):
        self.model = model
        self.timeseries = timeseries
        self.skip = skip
        self.real_trend = real_trend
        self.initial_money = initial_money
    
        self.minmax = minmax
        self.max_buy = max_buy
        self._initiate()

    def _initiate(self):
        # i assume first index is the close value
        self.trend = self.timeseries[0]
        self._mean = np.mean(self.trend)
        self._std = np.std(self.trend)
        self._inventory = []
        self._capital = self.initial_money
        self._queue = []
        self._scaled_capital = self.minmax.transform([[self._capital, 2,2,2,2,2,2,2,2,2,2,2]])[0, 0]
        self.units = 0 
        self.position = 'Flat'
        self.action = 'Hold'

    def reset_capital(self, capital):
        if capital:
            self._capital = capital
        self._scaled_capital = self.minmax.transform([[self._capital, 2,2,2,2,2,2,2,2,2,2,2]])[0, 0]
        self._queue = []
        self._inventory = []
        self.units = 0 
        self.position = 'Flat'
        self.action = 'Hold'

    def get_state(self, t, inventory, capital, timeseries):
        state = get_state(timeseries, t)
        len_inventory = len(inventory)
        if len_inventory:
            mean_inventory = np.mean(inventory)
        else:
            mean_inventory = 0
        z_inventory = (mean_inventory - self._mean) / self._std
        z_capital = (capital - self._mean) / self._std
        concat_parameters = np.concatenate(
            [state, [[len_inventory, z_inventory, z_capital]]], axis = 1
        )
        return concat_parameters
    
    async def evaluate_trade(self, data):
        scaled_data = self.minmax.transform([data])[0]
        self._queue.append(scaled_data)

        if len(self._queue) >= window_size:
            self._queue.pop(0)
        self._queue.append(scaled_data)

        if len(self._queue) < window_size:
            return {
                'status': 'data not enough to trade',
                'action': 'fail',
                'balance': self._capital,
                'timestamp': str(datetime.now()),
            }
        
        # Preparing state with inventory and capital considered
        state = self.get_state(window_size - 1, self._inventory, self._capital, timeseries = np.array(self._queue).T.tolist())

        # ONNX model inference
        input_name = self.model.get_inputs()[0].name
        ort_outs = self.model.run(None, {input_name: state.astype(np.float32)})

        decision_logits = ort_outs[0]
        #buy_signal = ort_outs[1]

        decision_probs = softmax(decision_logits)
        action = np.argmax(decision_probs, axis=1)[0]
        #prob = decision_probs[0][action]

        return {'action': int(action)}



    def change_data(self, timeseries, skip, initial_money, real_trend, minmax):
        self.timeseries = timeseries
        self.skip = skip
        self.initial_money = initial_money
        self.real_trend = real_trend
        self.minmax = minmax
        self._initiate()



    def get_reward(self, weights):
        initial_money = self._scaled_capital
        starting_money = initial_money
        invests = []
        self.model.weights = weights
        inventory = []
        state = self.get_state(0, inventory, starting_money, self.timeseries)

        for t in range(0, len(self.trend) - 1, self.skip):
            action = self.act(state)
            if action == 1 and starting_money >= self.trend[t]:
                inventory.append(self.trend[t])
                starting_money -= self.trend[t]

            elif action == 2 and len(inventory):
                bought_price = inventory.pop(0)
                starting_money += self.trend[t]
                invest = ((self.trend[t] - bought_price) / bought_price) * 100
                invests.append(invest)

            state = self.get_state(
                t + 1, inventory, starting_money, self.timeseries
            )
        invests = np.mean(invests)
        if np.isnan(invests):
            invests = 0
        score = (starting_money - initial_money) / initial_money * 100
        return invests * 0.7 + score * 0.3

    def fit(self, iterations, checkpoint):
        self.es.train(iterations, print_every = checkpoint)

    def buy(self):
        initial_money = self._scaled_capital
        starting_money = initial_money

        real_initial_money = self.initial_money
        real_starting_money = self.initial_money
        inventory = []
        real_inventory = []
        state = self.get_state(0, inventory, starting_money, self.timeseries)
        states_sell = []
        states_buy = []
        quantity = 0 
        f = .25/100
        for t in range(0, len(self.trend) - 1, self.skip):
            print(f"Day {t}, Inventory length: {len(inventory)}, start_mon: {starting_money}, Real_Starting Money: {real_starting_money} , quantity: {quantity}")

            action, prob = self.act_softmax(state)
            action0, buy  = self.act(state)
            print(t, prob)
            if action == 1 and starting_money >= self.trend[t] and t < (len(self.trend) - 1 - window_size) :
                if buy < 0:
                    buy = 1
                if buy > self.max_buy:
                    buy_units = self.max_buy
                else:
                    buy_units = buy 
                total_cost = buy_units*self.trend[t]*(1 + f)
                if starting_money >= total_cost:
                    total_buy = buy_units * self.real_trend[t]*(1 + f)
                    inventory.append(total_cost)
                    real_inventory.append(total_buy)
                    real_starting_money -= total_buy
                    starting_money -= total_cost
                    quantity += buy_units
                    states_buy.append(t)
                    print(
                        'day %d: buy %d units at price %f, total balance %f'
                        % (t, buy_units,total_buy, real_starting_money)
                    )
                else:
                    print(f'day {t}: not enough money to buy {buy_units} units at price {self.real_trend[t]}, total balance {real_starting_money}')
                    continue
            elif action == 2 and len(inventory):
                bought_price = inventory.pop(0)
                if quantity > self.max_buy:
                    sell_units = self.max_buy
                else:
                    sell_units = quantity
                if sell_units <1:
                    continue 
                quantity -= sell_units 
                total_sell = sell_units*self.real_trend[t]*(1 - f)
                real_bought_price = real_inventory.pop(0)
                starting_money += sell_units*self.trend[t]*(1 - f)
                real_starting_money += total_sell
                states_sell.append(t)
                try:
                    invest = (
                        (total_sell - real_bought_price)
                        / real_bought_price
                    ) * 100
                except:
                    invest = 0
                print(
                    'day %d, sell %d units at price %f, investment %f %%, total balance %f,'
                    % (t, sell_units,total_sell, invest, real_starting_money)
                )
            state = self.get_state(
                t + 1, inventory, starting_money, self.timeseries
            )

        invest = (
            (real_starting_money - real_initial_money) / real_initial_money
        ) * 100
        total_gains = real_starting_money - real_initial_money
        return states_buy, states_sell, total_gains, invest
