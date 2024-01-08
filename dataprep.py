import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
import logging
from datetime import timedelta
from typing import Dict
import numpy as np
from dataclasses import dataclass
from datetime import datetime, timedelta



# Change it together only
TIME_QUANTIZATION = '10min'
QUANT_TIMEDELTA = timedelta(minutes=10)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class DatasetBuilder:
    def __init__(self,
                 rows_limit: int,
                 buy_timedelta: timedelta,
                 sell_interval: (timedelta, timedelta),
                 change_magnitude: float
                 ):
        self.rows_limit = rows_limit
        self.buy_timedelta = buy_timedelta
        self.sell_interval = sell_interval
        self.change_magnitude = change_magnitude
        self.price_tables = None
        self.dataset = None

    def build_dataset(self):
        trades_df = get_db_df(self.rows_limit)
        trades_df = filter_by_program_id(trades_df)
        logging.info(f'Using memory: {trades_df.memory_usage(deep=True).sum() / 1024 ** 2} MB')
        add_time_quantization(trades_df)
        # trades_df = remove_rare_tokens(trades_df)
        trades_df = remove_latest_and_first_quant(trades_df)
        price_tables = get_price_tables(trades_df)
        self.price_tables = price_tables
        dataset = get_dataset_keys(trades_df)
        assign_labels(dataset, price_tables, self.buy_timedelta, self.sell_interval, self.change_magnitude)
        feature_calculator = FeatureCalculator(dataset, price_tables, trades_df, periods=14)
        feature_calculator.assign_features()
        dataset.sort_values(by=['time_quant', 'mint'], inplace=True)
        dataset.reset_index(drop=True, inplace=True)
        self.dataset = dataset
        return dataset

    def reassign_values(self,
                        buy_timedelta: timedelta,
                        sell_interval: (timedelta, timedelta),
                        change_magnitude: float):
        self.buy_timedelta = buy_timedelta
        self.sell_interval = sell_interval
        self.change_magnitude = change_magnitude
        assign_labels(self.dataset, self.price_tables, self.buy_timedelta, self.sell_interval, self.change_magnitude)
        return self.dataset


def get_db_df(rows_limit):
    logging.info('Getting data from db')

    load_dotenv()

    conn = psycopg2.connect(os.getenv('DATABASE_URL'))

    # Open a cursor to perform database operations
    cur = conn.cursor()

    # Execute a query
    cur.execute(f"""
    SELECT * FROM united_trades
    where 
    abs(sol_delta) > 0.03
    and source = 'sol'
    --abs(sol_delta) > 0.01
    --and abs(token_delta) > 0.01
    order by timestamp desc 
    limit {rows_limit}
    """)

    # Retrieve query results
    records = cur.fetchall()

    # Convert to pandas DataFrame
    df_from_db = pd.DataFrame(records, columns=[desc[0] for desc in cur.description])

    # Close communication with the database
    cur.close()
    conn.close()

    logging.info(f'Got {len(df_from_db)} records from db')
    logging.info(f'From {df_from_db.timestamp.min()} to {df_from_db.timestamp.max()}')

    return df_from_db


def filter_by_program_id(df_from_db):
    # The value to filter by, wrapped in a list to match the column's data type
    value_to_filter = ['JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4']
    # Use the `.apply()` method with a lambda function to check for equality
    filtered_df = df_from_db[df_from_db['program_ids'].apply(lambda x: x == value_to_filter)]
    logging.info(f'Filtered df by program_id, got {len(filtered_df)} records')
    return filtered_df


def add_time_quantization(trades_df):
    trades_df['time_quant'] = trades_df['timestamp'].dt.floor('10min')


def remove_rare_tokens(trades_df, min_trades=20, min_quants=5):
    # mints that appears in at least 10 trades
    mints_counts = trades_df.groupby('mint').size()
    mints_with_n_trades = mints_counts[mints_counts >= min_trades].index
    # print(mints_with_10_trades)
    # mints that appears in at least 3 pairs (mint, time_quant)
    keys_counts = trades_df.groupby(['mint', 'time_quant']).size()
    mints_with_n_quants = keys_counts[keys_counts >= min_quants].index.get_level_values(0).unique()
    # print(mints_with_3_pairs)
    # intersection of these mints
    mints_to_keep = mints_with_n_trades.intersection(mints_with_n_quants)

    # # sum of abs of sol_delta for each mint
    # mints_sol_delta = trades_df.groupby('mint')['sol_delta'].abs().agg('sum')
    # # mints that have at least 5 sol traded
    #

    # filter df by these mints
    logging.info('Before rare tokens filtering %d trades, %d mints', len(trades_df), trades_df.mint.nunique())
    trades_df = trades_df[trades_df.mint.isin(mints_to_keep)]
    logging.info('After rare tokens filtering %d trades, %d mints', len(trades_df), trades_df.mint.nunique())

    return trades_df


def remove_latest_and_first_quant(trades_df):
    min_quant = trades_df.time_quant.min()
    max_quant = trades_df.time_quant.max()
    trades_df = trades_df[trades_df.time_quant != min_quant]
    trades_df = trades_df[trades_df.time_quant != max_quant]
    return trades_df


class PriceTables:
    global_quants: pd.DatetimeIndex
    token_quants: Dict[str, pd.DatetimeIndex]
    sol_prices: Dict[str, np.array]
    united_prices: Dict[str, np.array]
    buy_volumes: Dict[str, np.array]
    sell_volumes: Dict[str, np.array]
    buy_cnts: Dict[str, np.array]
    sell_cnts: Dict[str, np.array]

    def __init__(self, sol_prices, united_prices, buy_volumes, sell_volumes, buy_cnts, sell_cnts, token_quants, global_quants):
        self.sol_prices = sol_prices
        self.united_prices = united_prices
        self.buy_volumes = buy_volumes
        self.sell_volumes = sell_volumes
        self.buy_cnts = buy_cnts
        self.sell_cnts = sell_cnts
        self.token_quants = token_quants
        self.global_quants = global_quants

    @staticmethod
    def get_quant_index(quant, quant_list):
        quant_index_float = (quant - quant_list[0]) / QUANT_TIMEDELTA
        # convert to int, but check that it's close to integer
        quant_index = round(quant_index_float)
        if abs(quant_index_float - quant_index) > 0.01:
            raise ValueError(f'Quant {quant} is not close to quantization')
        return quant_index

    def get_price(self, mint, quant, price_type):
        global_quant_index = self.get_quant_index(quant, self.global_quants)
        if global_quant_index < 0 or global_quant_index >= len(self.global_quants):
            return -1

        token_quant_index = self.get_quant_index(quant, self.token_quants[mint])
        if token_quant_index < 0:
            return 0.
        if token_quant_index >= len(self.token_quants[mint]):
            token_quant_index = len(self.token_quants[mint]) - 1

        if price_type == 'sol':
            return self.sol_prices[mint][token_quant_index]
        elif price_type == 'united':
            return self.united_prices[mint][token_quant_index]
        else:
            raise ValueError(f'Unknown price type: {price_type}')

    def get_volume(self, mint, quant, price_type):
        global_quant_index = self.get_quant_index(quant, self.global_quants)
        if global_quant_index < 0 or global_quant_index >= len(self.global_quants):
            return -1

        token_quant_index = self.get_quant_index(quant, self.token_quants[mint])
        if token_quant_index < 0:
            return 0.
        if token_quant_index >= len(self.token_quants[mint]):
            token_quant_index = len(self.token_quants[mint]) - 1

        if price_type == 'buy':
            return self.buy_volumes[mint][token_quant_index]
        elif price_type == 'sell':
            return self.sell_volumes[mint][token_quant_index]
        elif price_type == 'sum':
            return self.buy_volumes[mint][token_quant_index] + self.sell_volumes[mint][token_quant_index]
        else:
            raise ValueError(f'Unknown volume type: {price_type}')

    def get_trades_cnt(self, mint, quant, price_type):
        global_quant_index = self.get_quant_index(quant, self.global_quants)
        if global_quant_index < 0 or global_quant_index >= len(self.global_quants):
            return -1

        token_quant_index = self.get_quant_index(quant, self.token_quants[mint])
        if token_quant_index < 0:
            return 0.
        if token_quant_index >= len(self.token_quants[mint]):
            token_quant_index = len(self.token_quants[mint]) - 1

        if price_type == 'buy':
            return self.buy_cnts[mint][token_quant_index]
        elif price_type == 'sell':
            return self.sell_cnts[mint][token_quant_index]
        elif price_type == 'sum':
            return self.buy_cnts[mint][token_quant_index] + self.sell_cnts[mint][token_quant_index]
        else:
            raise ValueError(f'Unknown volume type: {price_type}')


def get_exponential_price_average(df, alpha=0.5) -> float:
    # calculates exponential moving average of price,
    # with sol_delta / token_delta as price,
    # and sol_delta as weight
    prices = abs(df.sol_delta / df.token_delta)
    weights = abs(df.sol_delta)
    # multiply weight exponentially
    exp = (1 - alpha) ** np.arange(len(df))
    # reverse last series
    exp = exp[::-1]
    # multiply weight by exp and make sum equal to 1
    weights = weights * exp
    weights = weights / weights.sum()
    # calc weighted average price
    price = (prices * weights).sum()
    return price


def get_buy_sell_volume(df) -> (float, float):
    buy_volume = 0.
    sell_volume = 0.

    for i, row in df.iterrows():
        if row['sol_delta'] > 0:
            sell_volume += row['sol_delta']
        if row['sol_delta'] < 0:
            buy_volume += -row['sol_delta']

    return buy_volume, sell_volume


def get_buy_sell_cnt(df) -> (float, float):
    buy_cnt = 0.
    sell_cnt = 0.

    for i, row in df.iterrows():
        if row['sol_delta'] > 0:
            buy_cnt += 1
        if row['sol_delta'] < 0:
            sell_cnt += 1

    return buy_cnt, sell_cnt


def get_price_tables(trades_df) -> PriceTables:
    min_global_quant = trades_df.time_quant.min()
    max_global_quant = trades_df.time_quant.max()
    global_quants = pd.date_range(min_global_quant, max_global_quant, freq=TIME_QUANTIZATION)

    trades_df.sort_values(by=['mint', 'time_quant'], inplace=True)
    sol_prices = {}
    united_prices = {}
    buy_volumes = {}
    sell_volumes = {}
    buy_cnts = {}
    sell_cnts = {}
    token_quants = {}

    for mint in trades_df.mint.unique():
        mint_df = trades_df[trades_df.mint == mint]
        mint_df.set_index('time_quant', inplace=True)
        last_sol_price = 0.
        last_united_price = 0.
        buy_volume_cumsum = 0.
        sell_volume_cumsum = 0.
        buy_cnt_cumsum = 0
        sell_cnt_cumsum = 0
        quants_in_df = mint_df.index.unique()

        min_token_quant = min(quants_in_df)
        max_token_quant = max(quants_in_df)
        token_quants[mint] = pd.date_range(min_token_quant, max_token_quant, freq=TIME_QUANTIZATION)
        token_quants_len = len(token_quants[mint])

        sol_prices[mint] = np.zeros(token_quants_len)
        united_prices[mint] = np.zeros(token_quants_len)
        buy_volumes[mint] = np.zeros(token_quants_len)
        sell_volumes[mint] = np.zeros(token_quants_len)
        buy_cnts[mint] = np.zeros(token_quants_len)
        sell_cnts[mint] = np.zeros(token_quants_len)

        for i, quant in enumerate(token_quants[mint]):
            if quant in quants_in_df:
                # mint_quant_df = mint_df[['source', 'sol_delta', 'token_delta']].loc[[quant]]
                mint_quant_df = mint_df.loc[[quant]]
                last_united_price = get_exponential_price_average(mint_quant_df)
                buy_volume, sell_volume = get_buy_sell_volume(mint_quant_df)
                buy_volume_cumsum += buy_volume
                sell_volume_cumsum += sell_volume
                buy_cnt, sell_cnt = get_buy_sell_cnt(mint_quant_df)
                buy_cnt_cumsum += buy_cnt
                sell_cnt_cumsum += sell_cnt
                # get only sol trades which means source is 'sol
                sol_df = mint_quant_df[mint_quant_df['source'] == 'sol']
                # if it's non-empty, calc price
                if len(sol_df) > 0:
                    last_sol_price = get_exponential_price_average(sol_df)
            sol_prices[mint][i] = last_sol_price
            united_prices[mint][i] = last_united_price
            buy_volumes[mint][i] = buy_volume_cumsum
            sell_volumes[mint][i] = sell_volume_cumsum
            buy_cnts[mint][i] = buy_cnt_cumsum
            sell_cnts[mint][i] = sell_cnt_cumsum
    logging.info(f'Got price tables for {len(sol_prices)} mints')
    return PriceTables(sol_prices, united_prices,
                       buy_volumes, sell_volumes,
                       buy_cnts, sell_cnts,
                       token_quants, global_quants)


def get_dataset_keys(trades_df):
    # get all pairs (mint, time_quant)
    keys = trades_df[['time_quant', 'mint']].drop_duplicates()
    # sort by mint, time_quant
    keys.sort_values(by=['time_quant', 'mint'], inplace=True)
    # reset index
    keys.reset_index(drop=True, inplace=True)
    return keys


def assign_labels(
        dataset: pd.DataFrame,
        price_tables,
        buy_timedelta: timedelta,
        sell_interval: (timedelta, timedelta),
        change_magnitude):
    dataset['price'] = 0.
    dataset['buy_price'] = 0.
    dataset['sell_price'] = 0.
    dataset['label'] = 'na'
    price_type = 'united'

    for i, row in dataset.iterrows():
        mint = row['mint']
        quant = row['time_quant']
        # price now
        now_price = price_tables.get_price(mint, quant, price_type)
        assert (now_price != -1)
        dataset.at[i, 'price'] = now_price
        # price after buy_timedelta
        buy_price = price_tables.get_price(mint, quant + buy_timedelta, price_type)
        if buy_price == -1:
            continue
        dataset.at[i, 'buy_price'] = buy_price
        # check that latest sell price exists
        latest_sell_price = price_tables.get_price(mint, quant + sell_interval[1], price_type)
        if latest_sell_price == -1:
            continue

        sell_prices = []
        for sell_quant in pd.date_range(quant + sell_interval[0], quant + sell_interval[1], freq=QUANT_TIMEDELTA):
            sell_price = price_tables.get_price(mint, sell_quant, price_type)
            assert (sell_price != -1)
            sell_prices.append(sell_price)

        assert (len(sell_prices) > 0)
        sell_price = np.average(sell_prices)

        dataset.at[i, 'sell_price'] = sell_price

        if now_price == 0:
            continue
        if sell_price > buy_price * change_magnitude:
            dataset.at[i, 'label'] = 'up'
        elif sell_price < buy_price / change_magnitude:
            dataset.at[i, 'label'] = 'down'
        else:
            dataset.at[i, 'label'] = 'flat'

    logging.info(f'Assigned labels to {len(dataset)} records')
    logging.info(str(dataset.label.value_counts()))


def cache_decorator(func):
    cached_key = None
    cached_result = None

    def wrapper(*args, **kwargs):
        nonlocal cached_key, cached_result

        # Create a key from the arguments and keyword arguments
        current_key = (args, tuple(kwargs.items()))

        # Check if the current arguments match the cached key
        if current_key == cached_key:
            return cached_result

        # Call the function and update the cache
        cached_result = func(*args, **kwargs)
        cached_key = current_key

        return cached_result

    return wrapper


@dataclass
class FeatureAggr:
    name: str
    delta: timedelta
    num_points: int


class FeatureCalculator:

    def __init__(
            self,
            dataset: pd.DataFrame,
            price_tables: PriceTables,
            trades_df: pd.DataFrame,
            periods: int
    ):
        self.dataset = dataset
        self.price_tables = price_tables
        self.trades_df = trades_df
        self.periods = periods

        self.aggregations = [
            FeatureAggr('now', timedelta(minutes=10), 1),
            FeatureAggr('10m', timedelta(minutes=10), self.periods),
            FeatureAggr('1h', timedelta(hours=1), self.periods),
            FeatureAggr('4h', timedelta(hours=4), self.periods),
            FeatureAggr('1d', timedelta(hours=24), self.periods),
        ]

    def assign_features(self):
        logging.info('Assigning features')
        self.dataset['day_of_week'] = self.dataset['time_quant'].dt.dayofweek
        self.dataset['hour'] = self.dataset['time_quant'].dt.hour

        # united price
        for a in self.aggregations:
            for trunc in ['', '_trunc']:
                self.dataset['found_points_' + a.name + trunc] = [-1.] * len(self.dataset)
                self.dataset['u_price_sma_' + a.name + trunc] = [-1.] * len(self.dataset)
                self.dataset['u_price_ema_' + a.name + trunc] = [-1.] * len(self.dataset)
                self.dataset['u_price_rsi_' + a.name + trunc] = [-1.] * len(self.dataset)
                for index, row in self.dataset.iterrows():
                    mint = row['mint']
                    time_quant = row['time_quant']
                    self.dataset.loc[index, 'found_points_' + a.name + trunc] = self.found_points('united', trunc, mint, time_quant, a.delta, a.num_points)
                    self.dataset.loc[index, 'u_price_sma_' + a.name + trunc] = self.sma_price('united', trunc, mint, time_quant, a.delta, a.num_points)
                    self.dataset.loc[index, 'u_price_ema_' + a.name + trunc] = self.ema_price('united', trunc, mint, time_quant, a.delta, a.num_points)
                    self.dataset.loc[index, 'u_price_rsi_' + a.name + trunc] = self.relative_strength_index('united', trunc, mint, time_quant, a.delta, a.num_points)

        # volume
        for mode in ['buy', 'sell', 'sum']:
            for a in self.aggregations:
                self.dataset[f'trcnt_sma_{mode}_{a.name}'] = [-1.] * len(self.dataset)
                self.dataset[f'vol_sma_{mode}_{a.name}'] = [-1.] * len(self.dataset)
                self.dataset[f'vol_ema_{mode}_{a.name}'] = [-1.] * len(self.dataset)
                for index, row in self.dataset.iterrows():
                    mint = row['mint']
                    time_quant = row['time_quant']
                    self.dataset.loc[index, f'trcnt_sma_{mode}_{a.name}'] = self.sma_trades(
                        mode, mint, time_quant, a.delta, a.num_points)
                    self.dataset.loc[index, f'vol_sma_{mode}_{a.name}'] = self.sma_volume(
                        mode, mint, time_quant, a.delta, a.num_points)
                    self.dataset.loc[index, f'vol_ema_{mode}_{a.name}'] = self.ema_volume(
                        mode, mint, time_quant, a.delta, a.num_points)
        logging.info('Features assigned')

    def get_base_price(self, mode, inp_mint, inp_quant):
        ans = self.price_tables.get_price(inp_mint, inp_quant, mode)
        assert (ans != -1)
        return ans

    def get_base_volume(self, mode, inp_mint, inp_quant):
        ans = self.price_tables.get_volume(inp_mint, inp_quant, mode)
        assert (ans != -1)
        return ans

    def get_base_trades_cnt(self, mode, inp_mint, inp_quant):
        ans = self.price_tables.get_trades_cnt(inp_mint, inp_quant, mode)
        assert (ans != -1)
        return ans

    @cache_decorator
    def get_price_points(self, mode, trunc, inp_mint, last_quant, interval, num_points):
        if trunc not in ['', '_trunc']:
            raise ValueError(f'Unknown trunc: "{trunc}"')
        ans = []
        for i in range(num_points):
            quant = last_quant - i * interval
            if quant < self.price_tables.global_quants[0]:
                break
            if trunc == '_trunc' and quant < self.price_tables.token_quants[inp_mint][0]:
                break
            ans.append(self.get_base_price(mode, inp_mint, quant))
        # reverse list
        ans = ans[::-1]
        return ans

    @cache_decorator
    def get_volume_points(self, mode, inp_mint, last_quant, interval, num_points):
        ans = []
        for i in range(num_points):
            quant = last_quant - i * interval
            prev_quant = quant - interval
            if prev_quant < self.price_tables.global_quants[0]:
                break
            volume = self.get_base_volume(mode, inp_mint, quant)
            prev_volume = self.get_base_volume(mode, inp_mint, prev_quant)
            ans.append(volume - prev_volume)
        # reverse list
        ans = ans[::-1]
        return ans

    @cache_decorator
    def get_trade_cnt_points(self, mode, inp_mint, last_quant, interval, num_points):
        ans = []
        for i in range(num_points):
            quant = last_quant - i * interval
            prev_quant = quant - interval
            if prev_quant < self.price_tables.global_quants[0]:
                break
            trades = self.get_base_trades_cnt(mode, inp_mint, quant)
            prev_trades = self.get_base_trades_cnt(mode, inp_mint, prev_quant)
            ans.append(trades - prev_trades)
        # reverse list
        ans = ans[::-1]
        return ans

    def found_points(self, mode, trunc, inp_mint, last_quant, interval, num_points):
        price_points = self.get_price_points(mode, trunc, inp_mint, last_quant, interval, num_points)
        return len(price_points)

    def sma_price(self, mode, trunc, inp_mint, last_quant, interval, num_points):
        price_points = self.get_price_points(mode, trunc, inp_mint, last_quant, interval, num_points)
        if len(price_points) == 0 or sum(price_points) == 0:
            return 0.
        return sum(price_points) / len(price_points) / self.get_base_price(mode, inp_mint, last_quant)

    def ema_price(self, mode, trunc, inp_mint, last_quant, interval, num_points):
        price_points = self.get_price_points(mode, trunc, inp_mint, last_quant, interval, num_points)
        if len(price_points) == 0 or sum(price_points) == 0:
            return 0.
        k = 2 / (num_points + 1)
        ans = price_points[0]
        for i in range(1, len(price_points)):
            ans = k * price_points[i] + (1 - k) * ans
        return ans / self.get_base_price(mode, inp_mint, last_quant)

    def relative_strength_index(self, mode, trunc, inp_mint, last_quant, interval, num_points):
        price_points = self.get_price_points(mode, trunc, inp_mint, last_quant, interval, num_points)
        if len(price_points) <= 1 or sum(price_points) == 0:
            return -1.
        gains = []
        losses = []
        for i in range(1, len(price_points)):
            diff = price_points[i] - price_points[i - 1]
            if diff > 0:
                gains.append(diff)
            elif diff < 0:
                losses.append(-diff)
        if len(gains) == 0:
            return 0.
        if len(losses) == 0:
            return 100.
        avg_gain = sum(gains) / len(gains)
        avg_loss = sum(losses) / len(losses)
        return 100 - 100 / (1 + avg_gain / avg_loss)

    def sma_trades(self, mode, inp_mint, last_quant, interval, num_points):
        trade_points = self.get_trade_cnt_points(mode, inp_mint, last_quant, interval, num_points)
        if len(trade_points) == 0:
            return 0.
        return sum(trade_points) / len(trade_points)

    def sma_volume(self, mode, inp_mint, last_quant, interval, num_points):
        volume_points = self.get_volume_points(mode, inp_mint, last_quant, interval, num_points)
        if len(volume_points) == 0:
            return 0.
        return sum(volume_points) / len(volume_points)

    def ema_volume(self, mode, inp_mint, last_quant, interval, num_points):
        volume_points = self.get_volume_points(mode, inp_mint, last_quant, interval, num_points)
        if len(volume_points) == 0:
            return 0.
        k = 2 / (num_points + 1)
        ans = volume_points[0]
        for i in range(1, len(volume_points)):
            ans = k * volume_points[i] + (1 - k) * ans
        return ans

