-- Витрина данных для анализа криптовалютного рынка
-- Вариант задания №30

DROP VIEW IF EXISTS crypto_market_datamart;

CREATE VIEW crypto_market_datamart AS
SELECT
    date,
    btc_price_close,
    eth_price_close,
    btc_market_cap,
    EXTRACT(YEAR FROM date) AS year
FROM
    stg_crypto_market
WHERE
    btc_price_close > 0
    AND eth_price_close > 0
    AND date IS NOT NULL;

COMMENT ON VIEW crypto_market_datamart IS
'Аналитическая витрина для дашборда криптовалютного рынка. Содержит цены BTC/ETH и рыночную капитализацию BTC.';