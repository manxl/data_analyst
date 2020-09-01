import pandas as pd
from dao.db_pool import get_engine
from conf.config import SCHEMA
import numpy as np


def get_stock_price_monthly(ts_code, trade_date):
    sql = "select * from stock_price_monthly where ts_code = '{}' and trade_date = '{}'"
    sql = sql.format(ts_code, trade_date)
    df = pd.read_sql_query(sql, get_engine())
    return df


def get_dividend(ts_code, start, end):
    sql = "select * from stock_dividend where ts_code= '{}' and ex_date between '{}' and '{}';"
    sql = sql.format(ts_code, start, end)
    df = pd.read_sql_query(sql, get_engine())
    return df


def get_trade_date(y, m):
    sql = '''select * from trade_date where y ={} and m = {};'''
    sql = sql.format(y, m)
    df = pd.read_sql_query(sql, get_engine())
    first = None if len(df) == 0 else df['first'][0]
    last = None if len(df) == 0 else df['last'][0]
    return first, last


def get_index_weight(index_code, y, m):
    pass


def get_index_distinct_codes(index_code):
    sql = "select DISTINCT(con_code) from index_weight where index_code = '{}'"
    sql = sql.format(index_code)
    df = pd.read_sql_query(sql, get_engine())
    if not len(df):
        print('error index_code:', index_code)
    return df


def get_list_all(limit=None, sql=None):
    sql = ("select * from stock_list " + (
        '' if limit is None else 'limit {}'.format(limit)) + ';') if sql is None else sql
    df = pd.read_sql_query(sql, get_engine())
    if not len(df):
        print('error sql:')
    return df

def get_analyse_collection():
    sql = """select
      l.ts_code,l.list_status,l.list_date,l.delist_date,
      b.y,b.m,
      b.total_share,b.total_liab,b.total_cur_assets,b.total_assets,
      f.dt_eps,f.eps,f.current_ratio,f.quick_ratio,
      m.close,
      d.stk_div,d.cash_div
from
      stock_list l,
      stock_balancesheet b,
      stock_fina_indicator f,
      stock_price_monthly m,
      stock_month_matrix_basic mb,
      stock_dividend d
where
      l.ts_code = b.ts_code and l.list_date <= b.end_date and
      b.ts_code = f.ts_code and b.y = f.y and b.m = f.m and
      b.ts_code = m.ts_code and b.y = m.y and  b.m =m.m and
      b.ts_code = d.ts_code and b.y = d.y and
      b.ts_code = mb.ts_code and b.y = mb.y and b.m =mb.m and
			mb.pe is not null and
		  b.y ={} and
			b.m ={};"""

    df = pd.read_sql_query(sql, get_engine())
    if not len(df):
        print('error sql:')
    return df

def init_table_indexes():
    sql = "SELECT TABLE_NAME FROM information_schema.TABLES where table_schema = '{}' and table_type = 'BASE TABLE';"
    sql = sql.format(SCHEMA)
    df = pd.read_sql_query(sql, get_engine())
    target_list = set(['end_date', 'index_code', 'con_code', 'ts_code', 'trade_date', 'y', 'm', 't', 'list_status',
                       'list_date', 'delist_date', 'total_share', 'total_liab', 'total_cur_assets', 'total_assets',
                       'dt_eps', 'current_ratio', 'quick_ratio', 'close', 'stk_div', 'cash_div'])

    sql_tpl_create_index = 'create index idx_{}_{} on {} ({});'
    e = get_engine()
    for table_name in df.iloc[:, 0]:
        sql_index = 'show index from {};'.format(table_name)
        sql_columns = 'show columns from {};'.format(table_name)
        df_index = pd.read_sql_query(sql_index, get_engine())
        df_columns = pd.read_sql_query(sql_columns, get_engine())
        df_index = set(df_index['Column_name'].values.tolist())
        df_columns = set(df_columns['Field'].values.tolist())
        target_columns = target_list & (df_columns - df_index)

        for column in target_columns:
            sql = sql_tpl_create_index.format(table_name, column, table_name, column)
            print(sql)
            e.execute(sql)

    print('-' * 32, 'add index')
    df = df[df.TABLE_NAME != 'trade_date_detail']
    df = df[df.TABLE_NAME != 'stock_dividend_detail']

    key_list = ['index_code', 'con_code', 'ts_code', 'cal_date', 'y', 'm']
    prim_keys = set(key_list)
    sql_prim = "select * from information_schema.TABLE_CONSTRAINTS  where CONSTRAINT_SCHEMA = '{}' and table_name = '{}';"
    sql_add_prim = 'alter table {}.{} add primary key({});'
    for table_name in df.iloc[:, 0]:
        sql = sql_prim.format(SCHEMA, table_name)
        prim = pd.read_sql_query(sql, get_engine())
        if len(prim) > 0:
            continue
        sql_columns = 'show columns from {};'.format(table_name)
        df_columns = pd.read_sql_query(sql_columns, get_engine())
        df_columns = set(df_columns['Field'].values.tolist())

        p = prim_keys & df_columns
        key_temp = key_list[:]
        for key in key_list:
            if not key in p:
                key_temp.remove(key)

        if key_temp:
            sql = sql_add_prim.format(SCHEMA, table_name, ','.join(key_temp))
            print(sql)
            # print(table_name, key_temp)
            e.execute(sql)




if __name__ == '__main__':
    init_table_indexes()
