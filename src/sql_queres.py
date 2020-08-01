import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('/Users/margaret/OneDrive/Documents/projects/option_price_collection/config.cfg')


# DROP TABLES
staging_options_table_drop = "DROP TABLE IF EXISTS staging_option;"
options_transaction_table_drop = "DROP TABLE IF EXISTS fact_options_transation;"
options_table_drop = "DROP TABLE IF EXISTS dim_contracts;"
tickers_table_drop = "DROP TABLE IF EXISTS dim_tickers;"


# CREATE TABLES
staging_options__table_create= ("""
CREATE TABLE IF NOT EXISTS staging_option (
          contractSymbol VARCHAR NOT NULL,
          lastTradeDate TIMESTAMP NOT NULL sortkey,
          strike NUMERIC,
          lastPrice NUMERIC,
          bid NUMERIC, 
          ask NUMERIC,
          change NUMERIC, 
          percentChange NUMERIC,
          volume NUMERIC,
          openInterest NUMERIC,
          impliedVolatility NUMERIC,
          inTheMoney BOOLEAN,
          contractSize VARCHAR,
          currency VARCHAR,
          Ticker VARCHAR NOT NULL distkey,
          OptionType VARCHAR(1) NOT NULL,
          contractExpiryDate VARCHAR(8) NOT NULL,
          Company VARCHAR,
          Exchange VARCHAR,
          TypeDisp VARCHAR);
""")

options_transation_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_options_transation(
          transaction_key INTEGER IDENTITY (0,1) PRIMARY KEY,
          contract_key INTEGER, 
          ticker_key INTEGER distkey,
          time TIMESTAMP NOT NULL sortkey,
          price NUMERIC,
          bid NUMERIC,
          ask NUMERIC,
          change NUMERIC,
          percent_change NUMERIC,
          volume INTEGER,
          open_interest INTEGER,
          implied_vol NUMERIC,
          in_the_money BOOLEAN,
          FOREIGN KEY(contract_key) REFERENCES dim_contracts(contract_key),
          FOREIGN KEY(ticker_key) REFERENCES dim_tickers(ticker_key));
""")

contracts_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_contracts (
          contract_key INTEGER IDENTITY (0,1) PRIMARY KEY, 
          contract_symbol VARCHAR NOT NULL,
          maturity_date DATE NOT NULL sortkey, 
          strike NUMERIC NOT NULL, 
          option_type VARCHAR(1) NOT NULL, 
          ticker_key INTEGER NOT NULL distkey, 
          currency VARCHAR, 
          contract_size VARCHAR,
          FOREIGN KEY(ticker_key) REFERENCES dim_tickers(ticker_key));
""")

tickers_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_tickers (
        ticker_key INTEGER IDENTITY (0,1) PRIMARY KEY, 
        ticker VARCHAR NOT NULL sortkey,
        company VARCHAR,
        exchange_nm VARCHAR)
diststyle all;
""")


# COPY TABLES
staging_options_copy = ("""
COPY staging_option FROM '{}'
CREDENTIALS 'aws_iam_role={}'
delimiter ',' gzip
ignoreheader 1
region 'us-east-2'
""").format(config.get('S3', 'OPTION_DATA'), config.get('IAM_ROLE', 'ARN'))


# INSERT INTO TABLES
tickers_table_insert = ("""INSERT INTO dim_tickers 
                                (ticker, company, exchange_nm)
                            SELECT distinct Ticker as ticker, 
                                   Company as company,
                                   Exchange as exchange_nm
                            FROM staging_option
""")

contracts_table_insert = ("""INSERT INTO dim_contracts 
                                (contract_symbol, maturity_date, strike, option_type, ticker_key, 
                                    currency, contract_size)
                             SELECT contractSymbol, 
                                    TO_DATE(contractExpiryDate,'YYYYMMDD'),
                                    strike,
                                    OptionType,
                                    ticker_key,
                                    currency,
                                    contractSize
                             FROM staging_option, dim_tickers
                             WHERE staging_option.ticker = dim_tickers.ticker
""")


options_transation_insert = ("""INSERT INTO fact_options_transation
                                    (contract_key, ticker_key, time, price, bid, ask, change, percent_change,
                                     volume, open_interest, implied_vol, in_the_money)
                                SELECT  contract_key,
                                        dim_tickers.ticker_key, 
                                        lastTradeDate,
                                        lastPrice,
                                        bid,
                                        ask,
                                        change,
                                        percentChange,
                                        volume::INTEGER, 
                                        openInterest::INTEGER,
                                        impliedVolatility,
                                        inTheMoney
                                    FROM staging_option, dim_contracts, dim_tickers
                                    WHERE staging_option.Ticker = dim_tickers.Ticker
                                        AND staging_option.contractSymbol = dim_contracts.contract_symbol
""")


# QUERIES 
drop_table_queries = [staging_options_table_drop, options_transaction_table_drop, \
                      options_table_drop, tickers_table_drop]
create_table_queries = [staging_options__table_create, tickers_table_create, 
                        contracts_table_create, options_transation_table_create]
copy_table_queries = [staging_options_copy]
insert_table_queries = [tickers_table_insert, contracts_table_insert, options_transation_insert]
