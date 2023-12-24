from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
import sys
from time import sleep
from typing import Any

import requests
import psycopg2
from dotenv import load_dotenv
import os
from psycopg2.extras import execute_batch


load_dotenv()

SLEEP_SEC = 0
# url is like NODE_URL=https://solana-mainnet.g.alchemy.com/v2/...
NODE_URL = os.getenv('NODE_URL')
STOP_AFTER_FOUND_TXS = 10
BUCKETS = 10000
SAVE_LESS_THAN_BUCKET = 1000
OLDEST_TX = datetime(2023, 11, 1, 0, 0)
PROCESS_TRADE_LESS_THAN_BUCKET = 100
TRADES_BATCH = 100
NODE_PARALLEL_REQUESTS = 5


def connect_to_db():
    return psycopg2.connect(os.getenv('DATABASE_URL'))


def retry_get_transactions_from_node(contract_address, limit=1000, before=''):
    while True:
        ans = get_transactions_from_node(contract_address, limit, before)
        if ans:
            return ans


def get_transactions_from_node(contract_address, limit=1000, before=''):
    headers = {"Content-Type": "application/json"}
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignaturesForAddress",
        "params": [
            contract_address,
            {
                "limit": limit,
                "commitment": "finalized",
            }
        ]
    }
    if before:
        body['params'][1]['before'] = before

    response = requests.post(NODE_URL, json=body, headers=headers)
    transactions = response.json().get('result', [])

    # Filter out transactions with errors
    successful_transactions = []
    for tx in transactions:
        if tx.get('err') is None:
            timestamp = datetime.utcfromtimestamp(tx['blockTime'])

            successful_transactions.append(
                {
                    'signature': tx['signature'],
                    'ts': timestamp
                }
            )

    return successful_transactions


def get_freshest_signatures(cur):
    cur.execute("SELECT signature FROM transactions order by timestamp desc limit 1000")
    return {row[0] for row in cur.fetchall()}


def get_oldest_tx(cur):
    cur.execute("SELECT signature, timestamp FROM transactions order by timestamp asc limit 1000")
    return [{'signature': row[0], 'ts': row[1]} for row in cur.fetchall()]


def insert_transactions(cur, transactions):
    query = "INSERT INTO transactions (signature, timestamp, bucket) VALUES (%s, %s, %s)"
    execute_batch(cur, query, transactions)


def get_unprocessed_transactions(cur, batch_size=TRADES_BATCH):
    query = """
    SELECT signature, timestamp FROM transactions 
    WHERE processed = FALSE AND bucket < %s 
    ORDER BY timestamp DESC 
    LIMIT %s
    """
    cur.execute(query, (PROCESS_TRADE_LESS_THAN_BUCKET, batch_size,))
    return [{'signature': row[0], 'ts': row[1]} for row in cur.fetchall()]


def node_txs_to_db(txs):
    transaction_data = []
    for tx in txs:
        bucket = hash(tx['signature']) % BUCKETS
        if bucket < SAVE_LESS_THAN_BUCKET:
            transaction_data.append((tx['signature'], tx['ts'], bucket))
    return transaction_data


# Function to process and store transactions

def loop_process_new_transactions(contract_address):
    while True:
        process_new_transactions(contract_address)
        sleep(10)


def process_new_transactions(contract_address):
    conn = connect_to_db()
    cur = conn.cursor()

    existing_signatures = get_freshest_signatures(cur)
    found_existing_txs = 0
    iteration = 0
    before = ''

    while True:
        iteration += 1
        new_transactions = retry_get_transactions_from_node(contract_address, before=before)
        print(f"Iteration {iteration}, {len(new_transactions)} txs, from {new_transactions[0]['ts']} to {new_transactions[-1]['ts']}")

        stored_txs = 0
        skipped_txs = 0

        kept_transactions = []

        for tx in new_transactions:
            if tx['signature'] in existing_signatures:
                skipped_txs += 1
                found_existing_txs += 1
                if found_existing_txs > STOP_AFTER_FOUND_TXS:
                    break
                continue

            kept_transactions.append(tx)
            before = tx['signature']

        transaction_data = node_txs_to_db(kept_transactions)
        stored_txs += len(transaction_data)

        if transaction_data:
            insert_transactions(cur, transaction_data)

        conn.commit()

        print(f"Iteration {iteration}, stored {stored_txs}, skipped {skipped_txs}")

        if found_existing_txs > STOP_AFTER_FOUND_TXS:
            print(f"Iteration {iteration}, stopped after {STOP_AFTER_FOUND_TXS} found transactions")
            break

    cur.close()
    conn.close()


def process_old_transactions(contract_address):
    conn = connect_to_db()
    cur = conn.cursor()
    iteration = 0

    while True:
        iteration += 1
        oldest_txs = get_oldest_tx(cur)
        if not oldest_txs:
            return
        oldest_signature = oldest_txs[0]['signature']
        oldest_ts = oldest_txs[0]['ts']
        signatures_set = {tx['signature'] for tx in oldest_txs}

        if oldest_ts < OLDEST_TX:
            print(f"Reached {OLDEST_TX}, stopping")
            break

        old_transactions = retry_get_transactions_from_node(contract_address, before=oldest_signature)
        print(f"Iteration {iteration}, {len(old_transactions)} txs, from {old_transactions[0]['ts']} to {old_transactions[-1]['ts']}")

        transaction_data = node_txs_to_db([tx for tx in old_transactions if tx['signature'] not in signatures_set])

        if transaction_data:
            insert_transactions(cur, transaction_data)

        conn.commit()
        print(f"Iteration {iteration}, stored {len(transaction_data)}")

    cur.close()
    conn.close()


def get_token_metadata(mint_address):
    if mint_address is None:
        return 'None'

    sleep(SLEEP_SEC)
    headers = {"Content-Type": "application/json"}
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [mint_address, {"encoding": "jsonParsed"}]
    }
    response = requests.post(NODE_URL, json=body, headers=headers)
    print(response.json())
    result = response.json().get('result', {})
    metadata = result.get('value', {}).get('data', {}).get('parsed', {}).get('info', {})
    return metadata.get('name', '???')


def convert_to_float(value):
    if isinstance(value, float):
        return value
    if isinstance(value, int):
        return float(value)
    return 0.


class RateLimitException(Exception):
    pass


def retry_get_transaction_details(tx):
    while True:
        try:
            return get_transaction_details(tx)
        except RateLimitException:
            sleep(1)


def get_transaction_details(tx):
    signature = tx['signature']
    headers = {"Content-Type": "application/json"}
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]
    }
    response = requests.post(NODE_URL, json=body, headers=headers)
    result = response.json().get('result', {})
    if not result:
        if 'Your app has exceeded its compute units per second capacity' in str(response.json()):
            raise RateLimitException(f"Rate limit exceeded for {signature}")
        raise Exception(response.json())

    block_time = result.get('blockTime')
    transaction_status = result.get('meta', {}).get('err')
    if transaction_status is not None:
        print(f"Transaction {signature} failed with error: {transaction_status}")
        raise Exception(f"Transaction {signature} failed with error: {transaction_status}")
    initiator = result['transaction']['message']['accountKeys'][0].get('pubkey')

    pre_token_balances = {b['accountIndex']: {
        'uiAmount': b['uiTokenAmount']['uiAmount'],
        'amount': b['uiTokenAmount']['amount'],
        'owner': b['owner'],
        'mint': b['mint'],
    } for b in result.get('meta', {}).get('preTokenBalances', [])}
    post_token_balances = {b['accountIndex']: {
        'uiAmount': b['uiTokenAmount']['uiAmount'],
        'amount': b['uiTokenAmount']['amount'],
        'owner': b['owner'],
        'mint': b['mint'],
    } for b in result.get('meta', {}).get('postTokenBalances', [])}

    tokens_transfers = []
    usd_delta = 0.
    sol_delta = 0.

    # Calculate and display non-zero deltas of token balances for the transaction initiator
    for index in pre_token_balances:
        if pre_token_balances[index]['owner'] != initiator:
            continue

        pre_balance = convert_to_float(pre_token_balances[index]['uiAmount'])
        post_balance = convert_to_float(post_token_balances.get(index, {}).get('uiAmount', 0))

        delta = post_balance - pre_balance

        if delta != 0:
            mint_address = pre_token_balances[index].get('mint')
            # token_name = get_token_metadata(mint_address)
            if mint_address in [
                'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                'USDH1SM1ojwWUga67PGrgFWUHibbjqMvuMaDkRJTgkX',
                'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
                'A1KLoBrKBde8Ty9qtNQUtq3C2ortoC3u7twggz7sEto6',
            ]:
                usd_delta += delta
            elif mint_address in [
                'So11111111111111111111111111111111111111112',
            ]:
                sol_delta += delta
            else:
                tokens_transfers.append({'delta': delta, 'mint': mint_address})

    for i, account in enumerate(result.get('transaction', {}).get('message', {}).get('accountKeys', [])):
        if account['pubkey'] == initiator:
            pre_sol_balance = convert_to_float(result.get('meta', {}).get('preBalances', [])[i])
            post_sol_balance = convert_to_float(result.get('meta', {}).get('postBalances', [])[i])
            sol_delta += (post_sol_balance - pre_sol_balance) / 10 ** 9

    if abs(sol_delta) < 0.001:
        sol_delta = 0

    # if tokens_transfers:
    #
    #     print(f"Transaction Signature: {signature}, Block Time: {block_time}, Initiator: {initiator}")
    #     for tt in tokens_transfers:
    #         print(f"Token Delta: {tt['delta']}, Mint: {tt['mint']}")
    #     print(f"USD Delta: {usd_delta}")
    #     print(f"SOL Delta: {sol_delta}")
    #     print()

    return {
        'signature': tx['signature'],
        'ts': tx['ts'],
        'transfers': tokens_transfers,
        'usd_delta': usd_delta,
        'sol_delta': sol_delta,
    }


def extract_trades(txs):
    with ThreadPoolExecutor(max_workers=NODE_PARALLEL_REQUESTS) as executor:
        results = list(executor.map(retry_get_transaction_details, txs))
        return results


@dataclass
class Trade:
    trades: int = 0
    token_volume: float = 0.
    usd_volume: float = 0.
    sol_volume: float = 0.
    sells: int = 0
    token_spent: float = 0.
    usd_got: float = 0.
    sol_got: float = 0.
    purchases: int = 0
    token_got: float = 0.
    usd_spent: float = 0.
    sol_spent: float = 0.


def aggregate_trades(trades):
    aggregated_trades: dict[Any, Trade] = {}
    for trade in trades:
        day = trade['ts'].date()
        if len(trade['transfers']) != 1:
            continue
        transfer = trade['transfers'][0]
        if transfer['delta'] == 0:
            continue
        if trade['sol_delta'] == 0 and trade['usd_delta'] == 0:
            continue

        mint = transfer['mint']
        key = (mint, day)

        agg_trade = aggregated_trades.get(key, Trade())

        agg_trade.trades += 1
        agg_trade.token_volume += abs(transfer['delta'])
        agg_trade.usd_volume += abs(trade['usd_delta'])
        agg_trade.sol_volume += abs(trade['sol_delta'])

        if transfer['delta'] > 0:
            # Buy
            agg_trade.purchases += 1
            agg_trade.token_got += transfer['delta']
            agg_trade.usd_spent -= trade['usd_delta']
            agg_trade.sol_spent -= trade['sol_delta']
        else:
            # Sell
            agg_trade.sells += 1
            agg_trade.token_spent -= transfer['delta']
            agg_trade.usd_got += trade['usd_delta']
            agg_trade.sol_got += trade['sol_delta']

        aggregated_trades[key] = agg_trade
    return aggregated_trades


def mark_tx_as_processed(cur, txs):
    update_query = "UPDATE transactions SET processed = TRUE WHERE signature = ANY(%s)"
    transaction_signatures = [tx['signature'] for tx in txs]
    cur.execute(update_query, (transaction_signatures,))


def apply_aggregated_trades(cur, aggregated_trades):
    for (mint, day), agg_trade in aggregated_trades.items():
        cur.execute("""
            INSERT INTO daily_trades (mint, day, trades, token_volume, usd_volume, sol_volume, sells, token_spent, usd_got, sol_got, purchases, token_got, usd_spent, sol_spent)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (mint, day) DO UPDATE SET
            trades = daily_trades.trades + EXCLUDED.trades,
            token_volume = daily_trades.token_volume + EXCLUDED.token_volume,
            usd_volume = daily_trades.usd_volume + EXCLUDED.usd_volume,
            sol_volume = daily_trades.sol_volume + EXCLUDED.sol_volume,
            sells = daily_trades.sells + EXCLUDED.sells,
            token_spent = daily_trades.token_spent + EXCLUDED.token_spent,
            usd_got = daily_trades.usd_got + EXCLUDED.usd_got,
            sol_got = daily_trades.sol_got + EXCLUDED.sol_got,
            purchases = daily_trades.purchases + EXCLUDED.purchases,
            token_got = daily_trades.token_got + EXCLUDED.token_got,
            usd_spent = daily_trades.usd_spent + EXCLUDED.usd_spent,
            sol_spent = daily_trades.sol_spent + EXCLUDED.sol_spent
            """, (mint, day, agg_trade.trades, agg_trade.token_volume, agg_trade.usd_volume, agg_trade.sol_volume, agg_trade.sells, agg_trade.token_spent, agg_trade.usd_got, agg_trade.sol_got, agg_trade.purchases, agg_trade.token_got, agg_trade.usd_spent, agg_trade.sol_spent))


def loop_process_trades():
    while True:
        processed = process_trades()
        if processed < 5:
            sleep(10)


def process_trades():
    conn = connect_to_db()
    cur = conn.cursor()
    txs = get_unprocessed_transactions(cur)
    if len(txs) == 0:
        print("No transactions to process")
        return 0
    print(f"Processing {len(txs)} transactions, from {txs[0]['ts']} to {txs[-1]['ts']}")
    trades = extract_trades(txs)
    print(f"Found {len(trades)} trades")
    aggregated_trades = aggregate_trades(trades)
    print(f"Got {len(aggregated_trades)} aggregated trades from {len(trades)} trades")
    mark_tx_as_processed(cur, txs)
    apply_aggregated_trades(cur, aggregated_trades)
    print(f"Saved to DB")
    conn.commit()
    cur.close()
    conn.close()
    return len(txs)


if __name__ == "__main__":
    contract_address = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'

    if len(sys.argv) > 1:

        if sys.argv[1] == 'scan_new_txs':
            loop_process_new_transactions(contract_address)
        elif sys.argv[1] == 'scan_old_txs':
            process_old_transactions(contract_address)
        elif sys.argv[1] == 'process_trades':
            loop_process_trades()
        else:
            print("Invalid command. Use 'scan_new_txs' or 'scan_old_txs'.")

    else:
        transaction_signatures = retry_get_transactions_from_node(contract_address)

        for sig in transaction_signatures:
            get_transaction_details(sig)
            sleep(SLEEP_SEC)
