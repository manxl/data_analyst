import pandas as pd
import dao.db_pool as pool

engine = pool.get_engine()


def get_stock_price_monthly(ts_code, trade_date):
    sql = "select * from stock_price_monthly where ts_code = '{}' and trade_date = '{}'"
    sql = sql.format(ts_code, trade_date)
    df = pd.read_sql_query(sql, engine)
    return df


def get_dividend(ts_code, start, end):
    sql = "select * from stock_dividend where ts_code= '{}' and ex_date between '{}' and '{}';"
    sql = sql.format(ts_code, start, end)
    df = pd.read_sql_query(sql, engine)
    return df


def get_trade_date(y, m):
    sql = '''select * from trade_date where y ={} and m = {};'''
    sql = sql.format(y, m)
    df = pd.read_sql_query(sql, engine)
    first = None if len(df) == 0 else df['first'][0]
    last = None if len(df) == 0 else df['last'][0]
    return first, last


def get_index_weight(index_code, y, m):
    pass


def get_index_distinct_codes(index_code):
    sql = "select DISTINCT(con_code) from index_weight where index_code = '{}'"
    sql = sql.format(index_code)
    df = pd.read_sql_query(sql, engine)
    if not len(df):
        print('error index_code:', index_code)
    return df
