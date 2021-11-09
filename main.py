import numpy as np
import pandas as pd
from pandas.io.sql import SQLTable
import pandas_datareader.data as web
import datetime
import sqlalchemy
import csv
from io import StringIO

from typing import List, Optional

pg_user = 'test_user'
pg_pass = 'test_password'
pg_url = 'localhost:5432'
pg_db = 'test_db'

cnx = sqlalchemy.create_engine(
    'postgresql://%s:%s@%s/%s' % (pg_user, pg_pass, pg_url, pg_db))


def get_yahoo_data() -> pd.DataFrame:
    try:
        df = pd.read_pickle('cache.pickle')
    except:
        tickers = ['AAPL', 'MSFT', 'SPY']
        start_date = datetime.datetime(2000, 1, 1)
        stop_date = datetime.datetime(2020, 1, 1)
        df_list = []
        for ticker in tickers:
            df = web.get_data_yahoo(ticker, start_date, stop_date)
            df['symbol'] = ticker
            df_list.append(df)

        df = pd.concat(df_list)
        df = df.rename(columns={'High': 'high', 'Low': 'low', 'Open': 'open',
                       'Close': 'close', 'Volume': 'volume', 'Adj Close': 'adj_close'})
        df.index.names = ['date']
        df.to_pickle('cache.pickle')
    return df


def increase_data(df: pd.DataFrame, n: int) -> pd.DataFrame:
    df_list = []
    for symbol, df_slice in df.groupby('symbol'):
        for i in range(n):
            _df_slice = df_slice.copy()
            _df_slice['symbol'] = '%s%u' % (symbol, i+1)
            df_list.append(_df_slice)
    return pd.concat(df_list)


def drop_table(name: str) -> None:
    try:
        cmd = 'DROP TABLE %s;' % name
        cnx.execute(cmd)
    except:
        pass


def create_symbol_table() -> None:
    cmd = '''
        CREATE TABLE symbols (
        symbol TEXT PRIMARY KEY,
        name TEXT
        );
        '''
    cnx.execute(cmd)


def create_historical_table() -> None:
    create_symbol_table()

    cmd = '''
        CREATE TABLE historical (
            time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            symbol TEXT,
            high NUMERIC,
            low NUMERIC,
            open NUMERIC,
            close NUMERIC,
            volume NUMERIC,
            adj_close NUMERIC,
            FOREIGN KEY (symbol) REFERENCES symbols (symbol),
            UNIQUE (time, symbol)
        );
        SELECT create_hypertable('historical', 'time');
        create index on historical (symbol, time desc);
        '''
    cnx.execute(cmd)


def reset_database() -> None:
    drop_table('historical')
    drop_table('symbols')
    create_historical_table()


def write_symbols(symbols: List[str]) -> None:
    formated_symbols = ',\n'.join("('%s')" % i.upper() for i in symbols)
    cmd = 'INSERT INTO symbols (symbol) VALUES\n%s\n' % formated_symbols
    cmd += 'ON CONFLICT DO NOTHING;'
    cnx.execute(cmd)


def write_historical(df: pd.DataFrame) -> None:
    def historical_method(table: SQLTable, conn: sqlalchemy.engine.Engine, keys: list, dataIter):
        with conn.connection.cursor() as cur:
            sBuf = StringIO()
            writer = csv.writer(sBuf)
            writer.writerows(dataIter)
            sBuf.seek(0)

            columns = ', '.join('"%s"' % k for k in keys)
            if table.schema:
                table_name = '%s.%s' % (table.schema, table.name)
            else:
                table_name = table.name
            temp_table_name = 'temp_%s' % table_name
            update_set = ", ".join(
                ['%s=EXCLUDED.%s' % (v, v) for v in keys if v not in ['time', 'symbol']])

            cmd = 'CREATE TEMPORARY TABLE %s (LIKE %s) ON COMMIT DROP;' % (
                temp_table_name, table_name)
            cur.execute(cmd)

            cmd = 'COPY %s (%s) FROM STDIN WITH CSV' % (
                temp_table_name, columns)
            cur.copy_expert(sql=cmd, file=sBuf)

            cmd = 'INSERT INTO %s(%s)\nSELECT %s FROM %s\n' % (
                table_name, columns, columns, temp_table_name)
            cmd += 'ON CONFLICT (time, symbol) DO UPDATE SET %s;' % update_set
            cur.execute(cmd)

            cur.execute('DROP TABLE %s' % temp_table_name)

    df.index.names = ['time']
    write_symbols(df['symbol'].unique())
    df.to_sql('historical', con=cnx, if_exists='append',
              index=True, method=historical_method)
    df.index.names = ['date']


def baseline_write(df: pd.DataFrame):
    df.index.names = ['time']
    write_symbols(df['symbol'].unique())
    df.to_sql('historical', con=cnx, if_exists='append', index=True)
    df.index.names = ['date']


def get_historical() -> pd.DataFrame:
    columns = ['symbol', 'time', 'high', 'low',
               'open', 'close', 'volume', 'adj_close']
    query = 'SELECT %s FROM historical' % (','.join('%s' % i for i in columns))

    s_buf = StringIO()
    conn = cnx.raw_connection()
    cur = conn.cursor()
    cmd = "COPY (%s) TO STDOUT WITH (FORMAT CSV, DELIMITER ',')" % query
    cur.copy_expert(cmd, s_buf)
    s_buf.seek(0)

    df = pd.read_csv(s_buf, names=columns, parse_dates=[
                     'time']).set_index('time').sort_index()
    df.index.names = ['date']
    return df


def temp():
    query = '''
    DELETE FROM historical
    WHERE symbol NOT IN ('SPY1')
    '''
    cnx.execute(query)


# def main2():
#     reset_database()
#     df = get_yahoo_data()
#     df1 = df[df['symbol'] == 'SPY']
#     df2 = df[df['symbol'] != 'SPY']
#     baseline_write(df)
#     df = get_historical()
#     print(df)
#     temp()
#     df = get_historical()
#     print(df)

#     quit()


def main():
    # get stock data from yahoo
    df = get_yahoo_data()
    df = increase_data(df, 100)

    # verify baseline and speedier are same
    reset_database()
    baseline_write(df)
    df1 = get_historical()

    reset_database()
    write_historical(df)
    df2 = get_historical()

    assert df1.equals(df2), 'data is different'

    # time baseline db write
    reset_database()
    baseline_write(df)
    df2 = df[df['symbol'] != 'SPY1']

    n = 50
    times = []
    for _ in range(n):
        start = datetime.datetime.now()
        temp()
        baseline_write(df2)
        times.append((datetime.datetime.now() - start).total_seconds())
    print('baseline average %6.2fs' % np.mean(times))

    # time speedier db write
    times = []
    for _ in range(n):
        start = datetime.datetime.now()
        temp()
        write_historical(df2)
        times.append((datetime.datetime.now() - start).total_seconds())
    print('speedier average %6.2fs' % np.mean(times))


if __name__ == '__main__':
    # main2()
    main()
