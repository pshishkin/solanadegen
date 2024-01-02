CREATE TABLE transactions (
    signature VARCHAR(100) PRIMARY KEY,
    timestamp TIMESTAMP,
    bucket INT,
    processed BOOLEAN DEFAULT FALSE,
    processed_single BOOLEAN DEFAULT FALSE
);

update transactions set processed = false;

CREATE INDEX idx_timestamp ON transactions (timestamp);

CREATE INDEX idx_bucket_timestamp ON transactions (bucket, processed, timestamp);



CREATE TABLE daily_trades (
    mint VARCHAR(100),
    day DATE,
    trades INT,
    token_volume FLOAT,
    usd_volume FLOAT,
    sol_volume FLOAT,
    sells INT,
    token_spent FLOAT,
    usd_got FLOAT,
    sol_got FLOAT,
    purchases INT,
    token_got FLOAT,
    usd_spent FLOAT,
    sol_spent FLOAT,
    PRIMARY KEY (mint, day)
);

CREATE TABLE trades (
    signature VARCHAR(100) PRIMARY KEY,
    mint VARCHAR(50),
    timestamp TIMESTAMP,
    trades INT,
    token_volume FLOAT,
    usd_volume FLOAT,
    sol_volume FLOAT,
    sells INT,
    token_spent FLOAT,
    usd_got FLOAT,
    sol_got FLOAT,
    purchases INT,
    token_got FLOAT,
    usd_spent FLOAT,
    sol_spent FLOAT
);

CREATE INDEX idx_mint ON trades (mint, timestamp);

CREATE TABLE subscribers (
    chat_id BIGINT PRIMARY KEY
);

CREATE TABLE broadcasted_tokens (
    mint VARCHAR(50) PRIMARY KEY
);


CREATE TABLE sol_trades (
    signature VARCHAR(100) PRIMARY KEY,
    mint VARCHAR(50),
    timestamp TIMESTAMP,
    token_delta FLOAT,
    sol_delta FLOAT
);

CREATE INDEX idx_sol_trades_1 ON sol_trades (mint, timestamp);
CREATE INDEX idx_sol_trades_2 ON sol_trades (timestamp);


CREATE TABLE token_trades (
    signature VARCHAR(100) PRIMARY KEY,
    timestamp TIMESTAMP,
    mint_spent VARCHAR(50),
    amount_spent FLOAT,
    mint_got VARCHAR(50),
    amount_got FLOAT,
    sol_delta FLOAT
);

CREATE INDEX idx_token_trades_1 ON token_trades (timestamp);

