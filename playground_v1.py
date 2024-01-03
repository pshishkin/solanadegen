import asyncio
import functools
import random
import traceback
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
import sys
from time import sleep
from typing import Any
from telegram import ForceReply, Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

import requests
import psycopg2
from dotenv import load_dotenv
import os
from psycopg2.extras import execute_batch, execute_values
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

SLEEP_SEC = 0
# url is like NODE_URL=https://solana-mainnet.g.alchemy.com/v2/...
NODE_URLS = [os.getenv('NODE_URL_1'), os.getenv('NODE_URL_2')]
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
STOP_AFTER_FOUND_TXS = 10
BUCKETS = 10000
SAVE_LESS_THAN_BUCKET = 1000
OLDEST_TX = datetime(2023, 11, 1, 0, 0)
PROCESS_TRADE_LESS_THAN_BUCKET = 1000
TRADES_BATCH = 300
NODE_PARALLEL_REQUESTS = 20


def get_node_url():
    return random.choice(NODE_URLS)


def retry_on_exception(exclude_exceptions=(KeyboardInterrupt,)):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            while True:
                try:
                    return func(*args, **kwargs)
                except exclude_exceptions as e:
                    raise e
                except Exception as e:
                    traceback.print_exc()
                    print(f"An exception occurred: {e}. Retrying...")
                    # print stacktrace
                    sleep(10)  # Wait for 1 second before retrying
        return wrapper
    return decorator

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

    response = requests.post(get_node_url(), json=body, headers=headers)
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


def get_unprocessed_individual_transactions(cur, batch_size=TRADES_BATCH):
    query = """
    SELECT signature, timestamp FROM transactions 
    WHERE processed = FALSE 
    AND bucket < %s
    and timestamp < now() - interval '3 minute'
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
@retry_on_exception()
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
        logging.info(f"Iteration {iteration}, {len(new_transactions)} txs, from {new_transactions[0]['ts']} to {new_transactions[-1]['ts']}")

        stored_txs = 0
        skipped_txs = 0

        kept_transactions = []

        for tx in new_transactions:
            before = tx['signature']

            if tx['signature'] in existing_signatures:
                skipped_txs += 1
                found_existing_txs += 1
                if found_existing_txs > STOP_AFTER_FOUND_TXS:
                    break
                continue

            kept_transactions.append(tx)

        transaction_data = node_txs_to_db(kept_transactions)
        stored_txs += len(transaction_data)

        if transaction_data:
            insert_transactions(cur, transaction_data)

        conn.commit()

        logging.info(f"Iteration {iteration}, stored {stored_txs}, skipped {skipped_txs}")

        if found_existing_txs > STOP_AFTER_FOUND_TXS:
            logging.info(f"Iteration {iteration}, stopped after {STOP_AFTER_FOUND_TXS} found transactions")
            break

    cur.close()
    conn.close()


@retry_on_exception()
def process_old_transactions(contract_address):
    conn = connect_to_db()
    cur = conn.cursor()
    iteration = 0
    oldest_signature = ''

    while True:
        iteration += 1
        oldest_txs = get_oldest_tx(cur)
        if not oldest_txs:
            return
        if not oldest_signature:
            oldest_signature = oldest_txs[0]['signature']
        oldest_ts = oldest_txs[0]['ts']
        signatures_set = {tx['signature'] for tx in oldest_txs}

        if oldest_ts < OLDEST_TX:
            logging.info(f"Reached {OLDEST_TX}, stopping")
            break

        old_transactions = retry_get_transactions_from_node(contract_address, before=oldest_signature)
        logging.info(f"Iteration {iteration}, {len(old_transactions)} txs, from {old_transactions[0]['ts']} to {old_transactions[-1]['ts']}")

        transaction_data = node_txs_to_db([tx for tx in old_transactions if tx['signature'] not in signatures_set])

        if transaction_data:
            insert_transactions(cur, transaction_data)

        conn.commit()
        logging.info(f"Iteration {iteration}, stored {len(transaction_data)}")
        oldest_signature = old_transactions[-1]['signature']

    cur.close()
    conn.close()


def get_token_metadata(mint_address):
    if mint_address is None:
        return 'None'

    # metadata_account = get_metadata_account(mint_address)

    headers = {"Content-Type": "application/json"}
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [mint_address, {"encoding": "jsonParsed"}]
    }
    response = requests.post(get_node_url(), json=body, headers=headers)
    logging.info(response.json())
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
            sleep(1.1)


def get_transaction_details(tx):
    signature = tx['signature']
    headers = {"Content-Type": "application/json"}
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]
    }
    response = requests.post(get_node_url(), json=body, headers=headers)
    result = response.json().get('result', {})
    if not result:
        if 'Your app has exceeded its compute units per second capacity' in str(response.json()) or \
                'credits limited to' in str(response.json()):
            raise RateLimitException(f"Rate limit exceeded for {signature}")
        raise Exception(response.json())

    block_time = result.get('blockTime')
    transaction_status = result.get('meta', {}).get('err')
    if transaction_status is not None:
        logging.info(f"Transaction {signature} failed with error: {transaction_status}")
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

    if abs(sol_delta) < 0.01:
        sol_delta = 0

    # if tokens_transfers and len(tokens_transfers) >= 1:
    #     logging.info(f"Transaction Signature: {signature}, Block Time: {block_time}, Initiator: {initiator}")
    #     for tt in tokens_transfers:
    #         logging.info(f"Token Delta: {tt['delta']}, Mint: {tt['mint']}")
    #     logging.info(f"SOL Delta: {sol_delta}")

    return {
        'signature': tx['signature'],
        'ts': tx['ts'],
        'transfers': tokens_transfers,
        'sol_delta': sol_delta,
        'trader': initiator,
    }


def extract_trades(txs):
    with ThreadPoolExecutor(max_workers=NODE_PARALLEL_REQUESTS) as executor:
        results = list(executor.map(retry_get_transaction_details, txs))
        return results


@dataclass
class SolTrade:
    signature: str = ''
    trader: str = ''
    mint: str = ''
    timestamp: datetime = None
    token_delta: float = 0.
    sol_delta: float = 0.


@dataclass
class TokenTrade:
    signature: str = ''
    trader: str = ''
    timestamp: datetime = None
    mint_spent: str = ''
    amount_spent: float = 0.
    mint_got: str = ''
    amount_got: float = 0.
    sol_delta: float = 0.


def mark_tx_as_processed(cur, txs):
    update_query = "UPDATE transactions SET processed = TRUE WHERE signature = ANY(%s)"
    transaction_signatures = [tx['signature'] for tx in txs]
    cur.execute(update_query, (transaction_signatures,))


def parse_sol_trade(trade):
    assert (len(trade['transfers']) == 1)
    parsed_trade = SolTrade()
    parsed_trade.signature = trade['signature']
    parsed_trade.trader = trade['trader']
    parsed_trade.mint = trade['transfers'][0]['mint']
    parsed_trade.timestamp = trade['ts']
    parsed_trade.token_delta = trade['transfers'][0]['delta']
    parsed_trade.sol_delta = trade['sol_delta']

    if abs(parsed_trade.sol_delta) == 0.:
        return None

    return (
        parsed_trade.signature,
        parsed_trade.trader,
        parsed_trade.mint,
        parsed_trade.timestamp,
        parsed_trade.token_delta,
        parsed_trade.sol_delta,
    )


def parse_token_trade(trade):
    assert (len(trade['transfers']) == 2)
    parsed_trade = TokenTrade()

    parsed_trade.signature = trade['signature']
    parsed_trade.trader = trade['trader']
    parsed_trade.mint = trade['transfers'][0]['mint']
    parsed_trade.timestamp = trade['ts']

    if trade['transfers'][0]['delta'] > 0 and trade['transfers'][1]['delta'] < 0:
        parsed_trade.mint_got = trade['transfers'][0]['mint']
        parsed_trade.amount_got = trade['transfers'][0]['delta']
        parsed_trade.mint_spent = trade['transfers'][1]['mint']
        parsed_trade.amount_spent = -trade['transfers'][1]['delta']
    elif trade['transfers'][0]['delta'] < 0 and trade['transfers'][1]['delta'] > 0:
        parsed_trade.mint_got = trade['transfers'][1]['mint']
        parsed_trade.amount_got = trade['transfers'][1]['delta']
        parsed_trade.mint_spent = trade['transfers'][0]['mint']
        parsed_trade.amount_spent = -trade['transfers'][0]['delta']
    else:
        return None

    return (
        parsed_trade.signature,
        parsed_trade.trader,
        parsed_trade.timestamp,
        parsed_trade.mint_spent,
        parsed_trade.amount_spent,
        parsed_trade.mint_got,
        parsed_trade.amount_got,
        parsed_trade.sol_delta,
    )


def apply_sol_trades(cur, trades):
    trade_values = [parse_sol_trade(trade) for trade in trades]
    trade_values = [tv for tv in trade_values if tv is not None]

    insert_query = """
    INSERT INTO sol_trades (
        signature, trader, mint, timestamp, token_delta, sol_delta
    ) VALUES %s ON CONFLICT (signature) 
    DO UPDATE SET 
        trader = EXCLUDED.trader, 
        mint = EXCLUDED.mint, 
        timestamp = EXCLUDED.timestamp, 
        token_delta = EXCLUDED.token_delta, 
        sol_delta = EXCLUDED.sol_delta
    """

    # Using psycopg2's execute_values for bulk insert
    execute_values(cur, insert_query, trade_values)


def apply_token_trades(cur, trades):
    trade_values = [parse_token_trade(trade) for trade in trades]
    trade_values = [tv for tv in trade_values if tv is not None]

    insert_query = """
    INSERT INTO token_trades (
        signature, trader, timestamp, mint_spent, amount_spent, mint_got, amount_got, sol_delta
    ) VALUES %s ON CONFLICT (signature) 
    DO UPDATE SET 
        trader = EXCLUDED.trader, 
        timestamp = EXCLUDED.timestamp, 
        mint_spent = EXCLUDED.mint_spent, 
        amount_spent = EXCLUDED.amount_spent, 
        mint_got = EXCLUDED.mint_got, 
        amount_got = EXCLUDED.amount_got, 
        sol_delta = EXCLUDED.sol_delta
    """

    # Using psycopg2's execute_values for bulk insert
    execute_values(cur, insert_query, trade_values)

def add_subscriber(chat_id):
    conn = connect_to_db()
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO subscribers (chat_id) VALUES (%s) ON CONFLICT DO NOTHING", (chat_id,))
        conn.commit()
    finally:
        cur.close()
        conn.close()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    add_subscriber(update.message.chat_id)
    await update.message.reply_html(
        rf"Hi {user.mention_html()}! I'll send you hyping degen bots as soon as they appear.",
    )

    conn = connect_to_db()
    cur = conn.cursor()
    mints = get_freshest_hyping_mints(cur)
    logging.info(f"Got {len(mints)} mints to send")
    text = "A few freshest hyping tokens:\n"
    for m in mints:
        text += f"[{m[0][:5]}...{m[0][-5:]}](https://solscan.io/token/{m[0]}) since {m[1]} [Ape](https://jup.ag/swap/USDC-{m[0]})!\n"
    await update.message.reply_markdown(
        text=text,
        disable_web_page_preview=True,
    )
    cur.close()
    conn.close()


@retry_on_exception()
def loop_process_bot_subscriptions():
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, start))
    application.run_polling(allowed_updates=Update.ALL_TYPES)


def get_subscribers(cur):
    cur.execute("SELECT chat_id FROM subscribers")
    subscribers = {row[0] for row in cur.fetchall()}
    return subscribers


broadcast_mints_query = """
SELECT dt.mint, min(dt.day) as first_trade_day
FROM daily_trades dt
WHERE dt.mint NOT IN (SELECT mint FROM broadcasted_tokens)
GROUP BY dt.mint
HAVING SUM(dt.trades) * 325 > 30000
LIMIT 1
"""


def get_hyping_mints_for_broadcast(cur):
    cur.execute(broadcast_mints_query)
    mints = [(row[0], row[1]) for row in cur.fetchall()]
    return mints


freshest_mints_query = """
SELECT dt.mint, min(dt.day) as first_trade_day
FROM daily_trades dt
GROUP BY dt.mint
HAVING SUM(dt.trades) * 325 > 30000
order by 2 desc
LIMIT 10
"""


def get_freshest_hyping_mints(cur):
    cur.execute(freshest_mints_query)
    mints = [(row[0], row[1]) for row in cur.fetchall()]
    return mints


def mark_mints_as_broadcasted(cur, conn, mints):
    query = "INSERT INTO broadcasted_tokens (mint) VALUES %s ON CONFLICT DO NOTHING"
    execute_values(cur, query, [(mint[0],) for mint in mints])
    conn.commit()


@retry_on_exception()
async def loop_process_bot_broadcasts():
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    conn = connect_to_db()
    cur = conn.cursor()

    try:
        while True:
            subscribers = get_subscribers(cur)
            mints = get_hyping_mints_for_broadcast(cur)
            logging.info(f"Got {len(mints)} mints to broadcast")
            for s in subscribers:
                for m in mints:
                    await application.bot.send_message(
                        chat_id=s,
                        text=f"Token [{m[0][:5]}...{m[0][-5:]}](https://solscan.io/token/{m[0]}) is hyping since {m[1]}\n[Ape into it!](https://jup.ag/swap/USDC-{m[0]})",
                        disable_web_page_preview=True,
                        parse_mode='Markdown'
                    )
            mark_mints_as_broadcasted(cur, conn, mints)
            sleep(60)
    finally:
        cur.close()
        conn.close()


@retry_on_exception()
def loop_process_individual_trades():
    while True:
        processed = process_individual_trades()
        if processed == 0:
            break


def process_individual_trades():
    logging.info(f"Getting unprocessed transactions from DB")
    conn = connect_to_db()
    cur = conn.cursor()
    txs = get_unprocessed_individual_transactions(cur)
    if len(txs) == 0:
        logging.info("No transactions to process")
        return 0
    logging.info(f"Processing {len(txs)} transactions, from {txs[0]['ts']} to {txs[-1]['ts']}")
    trades = extract_trades(txs)

    sol_trades = [t for t in trades if len(t['transfers']) == 1]
    token_trades = [t for t in trades if len(t['transfers']) == 2]
    logging.info(f"Saving to DB {len(trades)} trades: {len(sol_trades)} SOL trades, {len(token_trades)} token trades")
    apply_sol_trades(cur, sol_trades)
    apply_token_trades(cur, token_trades)
    mark_tx_as_processed(cur, txs)
    conn.commit()
    logging.info(f"Saved to DB")
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
        elif sys.argv[1] == 'process_individual_trades':
            loop_process_individual_trades()
        elif sys.argv[1] == 'bot_start':
            loop_process_bot_subscriptions()
        elif sys.argv[1] == 'bot_broadcast':
            asyncio.run(loop_process_bot_broadcasts())
        else:
            logging.info("Invalid command. Use 'scan_new_txs' or 'scan_old_txs'.")

    else:
        transaction_signatures = get_token_metadata('mb1eu7TzEc71KxDpsmsKoucSSuuoGLv1drys1oP2jh6')
