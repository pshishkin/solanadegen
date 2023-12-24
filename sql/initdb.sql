CREATE TABLE transactions (
    signature VARCHAR(100) PRIMARY KEY,
    timestamp TIMESTAMP,
    bucket INT,
    processed BOOLEAN DEFAULT FALSE
);

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

