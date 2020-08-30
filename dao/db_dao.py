import pandas as pd
from dao.db_pool import get_engine
from conf.config import SCHEMA

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


def get_list_all():
    sql = "select * from stock_list;"
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


if __name__ == '__main__':
    init_table_indexes()
