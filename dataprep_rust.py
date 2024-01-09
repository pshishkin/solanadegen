import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
import logging
from typing import Dict
import numpy as np
from dataclasses import dataclass
from datetime import datetime, timedelta
import rust_py_extension
from io import StringIO


# Change it together only
TIME_QUANTIZATION = '10min'
QUANT_TIMEDELTA = timedelta(minutes=10)
QUANT_TIMEDELTA_SECONDS = QUANT_TIMEDELTA.total_seconds()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class DatasetBuilder:
    def __init__(self,
                 rows_limit: int,
                 quant_timedelta: timedelta,
                 buy_timedelta: timedelta,
                 sell_params: (timedelta, timedelta, timedelta),
                 change_magnitude: float
                 ):
        self.rows_limit = rows_limit
        self.quant_timedelta = quant_timedelta
        self.buy_timedelta = buy_timedelta
        self.sell_params = sell_params
        self.change_magnitude = change_magnitude
        self.trades_df = None

    def build_dataset(self):
        check_rust()
        trades_df = get_db_df(self.rows_limit)
        trades_df = filter_by_program_id(trades_df)
        self.trades_df = trades_df
        logging.info(f'Using memory: {trades_df.memory_usage(deep=True).sum() / 1024 ** 2} MB')
        dataset = self._build_dataset()
        return dataset

    def rebuild_dataset(self,
                        buy_timedelta: timedelta,
                        sell_params: (timedelta, timedelta, timedelta),
                        change_magnitude: float):
        self.buy_timedelta = buy_timedelta
        self.sell_params = sell_params
        self.change_magnitude = change_magnitude
        dataset = self._build_dataset()
        return dataset

    def _build_dataset(self):
        dataset = self.get_rust_ds(self.trades_df)
        dataset['timestamp'] = pd.to_datetime(dataset['timestamp'])
        dataset = assign_labels(dataset, self.change_magnitude)
        dataset.sort_values(by=['timestamp', 'mint'], inplace=True)
        dataset.reset_index(drop=True, inplace=True)
        logging.info('Done')
        return dataset

    def get_rust_ds(self, send_df):
        send_df = send_df[['timestamp', 'mint', 'token_delta', 'sol_delta']]
        csv_data = send_df.to_csv(index=False)

        # Call Rust function
        result, received_csv_data = rust_py_extension.process_dataframe(
            csv_data,
            round(self.quant_timedelta.total_seconds()),
            round(self.buy_timedelta.total_seconds()),
            round(self.sell_params[0].total_seconds()),
            round(self.sell_params[1].total_seconds()),
            round(self.sell_params[2].total_seconds()),
        )
        logging.info(result)
        logging.info(f"Got {len(received_csv_data) / 1024 ** 2}MB from Rust")
        received_df = pd.read_csv(StringIO(received_csv_data))
        return received_df


def check_rust():
    array = np.array([1.0, 2.0, 3.0])
    result = rust_py_extension.multiply_by_two(array)
    logging.info("Check rust, Original array: %s", array)
    logging.info("Check rust, Processed array: %s", result)


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


def assign_labels(
        received_df: pd.DataFrame,
        change_magnitude
):
    logging.info('Assigning labels')
    received_df['label'] = 'na'
    received_df['upside'] = received_df['sell_price'] / received_df['buy_price'] - 1
    cols = list(received_df)
    cols.insert(5, cols.pop(cols.index('upside')))
    received_df = received_df.loc[:, cols]
    cols = list(received_df)
    cols.insert(6, cols.pop(cols.index('label')))
    received_df = received_df.loc[:, cols]

    received_df.loc[(received_df['sell_price'] > 0), 'label'] = 'flat'
    received_df.loc[(received_df['upside'] + 1 > change_magnitude) & (received_df['sell_price'] > 0), 'label'] = 'up'
    received_df.loc[(received_df['upside'] + 1 < 1. / change_magnitude) & (received_df['sell_price'] > 0), 'label'] = 'down'

    return received_df