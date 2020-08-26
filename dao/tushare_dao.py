import numpy as np
import pandas as pd
import tushare as ts
from sqlalchemy.types import VARCHAR,  Integer, DATE, DECIMAL, INT, BIGINT
import conf.config as config
import dao.db_pool as pool
import dao.db_dao as dao
import time
from concurrent.futures import ThreadPoolExecutor

__pro = ts.pro_api()
__engine = pool.get_engine()


def df_add_y_m(df, column_name):
    df['y'] = df[column_name].apply(lambda s: int(s[:4]))
    df['m'] = df[column_name].apply(lambda s: int(s[4:6]))


def need_pull_check(code, table_name, force=False, condition_column='ts_code'):
    if not force:
        sql = "select count(*) from {} where {} = '{}';".format(table_name, condition_column, code)
        try:
            size = __engine.execute(sql).fetchone()[0]
        except Exception as e:
            if 'f405' == e.code:
                return True
            else:
                print(e)
                exit(4)
        return False if size > 0 else True
    else:
        sql = "delete from {} where {} = '{}';".format(table_name, condition_column, code)
        try:
            r = __engine.execute(sql)
            print('force clean {} rows 4 {}'.format(r.rowcount, code))
        except Exception as e:
            if 'f405' == e.code:
                return True
            else:
                print(e)
                exit(4)

        return True


def init_stock_list_all():
    fileds = 'ts_code,symbol,name,area,industry,fullname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs'
    d_l = __pro.stock_basic(exchange='', list_status='L', fields=fileds)
    print('L', len(d_l))
    d_d = __pro.stock_basic(exchange='', list_status='D', fields=fileds)
    print('D', len(d_d))
    d_p = __pro.stock_basic(exchange='', list_status='P', fields=fileds)
    print('P', len(d_p))
    df = d_l.append(d_d).append(d_p)
    print('all size:', len(df))

    dtype = {'ts_code': VARCHAR(length=10), 'symbol': VARCHAR(length=8), 'name': VARCHAR(length=20),
             'area': VARCHAR(length=10), 'industry': VARCHAR(length=32), 'fullname': VARCHAR(length=32),
             'market': VARCHAR(length=10), 'exchange': VARCHAR(length=10), 'curr_type': VARCHAR(length=5),
             'list_status': VARCHAR(length=1), 'list_date': DATE(), 'delist_date': DATE(),
             'is_hs': VARCHAR(length=1)}

    # df.reset_index(drop=True)
    # df = df.reindex(columns='ts_code,end_date,ex_date,div_proc,stk_div,cash_div'.split(','))
    df.to_sql('stock_list', __engine, dtype=dtype, index=False, if_exists='replace')


def init_stock_index(index_code, force=False):
    table_name = 'index_weight'

    if not need_pull_check(index_code, table_name, force, condition_column='index_code'):
        return

    y_start = 1990

    __pool = ThreadPoolExecutor(max_workers=config.MULTIPLE, thread_name_prefix="test_")
    fs = []
    i = 0
    for y_i in range(31)[::-1]:
        y = y_start + y_i
        first, last = dao.get_trade_date(y, 0)
        if not first:
            continue
        print("{}-{}".format(y, 0))
        first = first.strftime('%Y%m%d')
        last = last.strftime('%Y%m%d')
        f1 = __pool.submit(__pro.index_weight, index_code=index_code, start_date=first, end_date=first)
        f2 = __pool.submit(__pro.index_weight, index_code=index_code, start_date=last, end_date=last)
        fs.append(f1)
        fs.append(f2)
        i += 2
        if i > 197:
            print('198次后休息60秒')
            time.sleep(60)
            i = 0

    df = None
    for f2 in fs:
        temp_df = f2.result()
        if len(temp_df):
            if df is None:
                df = temp_df
            else:
                df = df.append(temp_df, ignore_index=True)

    df_add_y_m(df, 'trade_date')

    dtype = {'index_code': VARCHAR(length=10), 'con_code': VARCHAR(length=10), 'y': INT, 'm': INT,
             'trade_date': DATE(), 'weight': DECIMAL(precision=10, scale=6)}

    df = df.reindex(columns='index_code,con_code,y,m,trade_date,weight'.split(','))

    df.to_sql(table_name, __engine, dtype=dtype, index=False, if_exists='append')


def init_trade_date():
    template_start = '{}00101'
    template_end = '{}91231'
    data = None
    for i in range(4):
        print(i)
        t = 199 + i
        start, end = template_start.format(t), template_end.format(t)
        df = __pro.query('trade_cal', start_date=start, end_date=end)
        if data is not None:
            data = data.append(df, ignore_index=True)
        else:
            data = df
        print('start:{},date:{}'.format(start, len(data)))

    # data.to_sql('trade_date_o', get_engine(), if_exists='replace', schema=db_name)
    df = data
    df_add_y_m(df, 'cal_date')
    # df['y'] = df['cal_date'].apply(lambda s: int(s[:4]))
    # df['m'] = df['cal_date'].apply(lambda s: int(s[4:6]))

    df.set_index(['y', 'm', 'cal_date'])
    df = df[df['is_open'] == 1]
    df = df.reindex(columns=['y', 'm', 'cal_date', 'is_open', 'exchange'])
    df.to_sql('trade_date_detail',
              __engine,
              index=False,
              dtype={'cal_date': DATE(), 'y': Integer(), 'm': INT(), 'is_open': INT(), 'exchange': VARCHAR(8)},
              # dtype={'cal_date': 'M8[d]'},
              if_exists='replace')
    '''
    分组插入扩展表
    '''
    grouped_m = df.groupby(['y', 'm'])
    # for a, g in grouped_m:
    #     print(a)
    #     print(g)
    r1 = grouped_m['cal_date'].agg([np.min, np.max])
    r1 = r1.rename(columns={'amin': 'first', 'amax': 'last'})
    r1['y'] = pd.Series(r1.index.get_level_values('y'), index=r1.index)
    r1['m'] = pd.Series(r1.index.get_level_values('m'), index=r1.index)

    grouped_m = df.groupby(['y'])
    r2 = grouped_m['cal_date'].agg([np.min, np.max])
    r2 = r2.rename(columns={'amin': 'first', 'amax': 'last'})
    r2['y'] = pd.Series(r2.index.get_level_values('y'), index=r2.index)
    r2['m'] = pd.Series(np.zeros(len(r2)), index=r2.index)

    r = r1.append(r2, ignore_index=True)
    r = r.reindex(columns=['y', 'm', 'first', 'last'])
    r.to_sql('trade_date', __engine,
             index=False,
             dtype={'first': DATE(), 'last': DATE(), 'y': Integer(), 'm': INT()}
             , if_exists='replace')


def init_stock_price_monthly(ts_code, force=False):
    table_name = 'stock_price_monthly'

    if not need_pull_check(ts_code, table_name, force):
        return

    df = __pro.monthly(ts_code=ts_code, fields='ts_code,trade_date,open,high,low,close,vol,amount')
    if not len(df):
        return

    df_add_y_m(df, 'trade_date')
    dtype = {'ts_code': VARCHAR(length=10), 'trade_date': DATE(), 'y': INT, 'm': INT,
             'open': DECIMAL(precision=8, scale=2), 'high': DECIMAL(precision=8, scale=2),
             'low': DECIMAL(precision=8, scale=2), 'close': DECIMAL(precision=8, scale=2),
             'vol': BIGINT(), 'amount': BIGINT()}
    df.to_sql(table_name, __engine, dtype=dtype, index=False, if_exists='append')


def init_dividend(ts_code, force=False):
    table_name = 'stock_dividend'

    if not need_pull_check(ts_code, table_name, force):
        print('need not 2 pull {} -> {}'.format(table_name, ts_code))
        return
    else:
        print('start 2 pull {} -> {} income .'.format(table_name, ts_code))

    df = __pro.dividend(ts_code=ts_code, fields='ts_code,end_date,div_proc,stk_div,cash_div,ex_date')
    df = df[df['div_proc'].str.contains('实施')]
    df.reset_index(drop=True)
    df = df.reindex(columns='ts_code,end_date,ex_date,div_proc,stk_div,cash_div'.split(','))
    dtype = {'ts_code': VARCHAR(length=10), 'end_date': DATE(), 'div_proc': VARCHAR(length=10),
             'stk_div': DECIMAL(precision=10, scale=8), 'cash_div': DECIMAL(precision=12, scale=8),
             'ex_date': DATE()}

    df.to_sql(table_name, __engine, dtype=dtype, index=False, if_exists='append')


def init_income(ts_code, force=False):
    table_name = 'stock_income'

    if not need_pull_check(ts_code, table_name, force):
        print('need not 2 pull {} -> {}'.format(table_name, ts_code))
        return
    else:
        print('start 2 pull {} -> {} income .'.format(table_name, ts_code))

    # df = pro.income(ts_code='600000.SH', start_date='20180101', end_date='20180730', fields='ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,basic_eps,diluted_eps')
    df = __pro.income(ts_code=ts_code, start_date='19901201', end_date='20210101')
    df.reset_index(drop=True)
    df.to_sql(table_name, __engine, index=False, if_exists='append')


def init_balancesheet(ts_code, force=False):
    table_name = 'stock_balancesheet'

    if not need_pull_check(ts_code, table_name, force):
        print('need not 2 pull {} -> {}'.format(table_name, ts_code))
        return
    else:
        print('start 2 pull {} -> {} income .'.format(table_name, ts_code))

    print('start 2 pull {} income .'.format(ts_code))
    # df = pro.income(ts_code='600000.SH', start_date='20180101', end_date='20180730', fields='ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,basic_eps,diluted_eps')
    df = __pro.balancesheet(ts_code=ts_code, start_date='19901201', end_date='20210101')
    df.reset_index(drop=True)
    df.to_sql(table_name, __engine, index=False, if_exists='append')


def init_cashflow(ts_code, force=False):
    table_name = 'stock_cashflow'

    if not need_pull_check(ts_code, table_name, force):
        print('need not 2 pull {} -> {}'.format(table_name, ts_code))
        return
    else:
        print('start 2 pull {} -> {} income .'.format(table_name, ts_code))

    # df = pro.income(ts_code='600000.SH', start_date='20180101', end_date='20180730', fields='ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,basic_eps,diluted_eps')
    df = __pro.cashflow(ts_code=ts_code, start_date='19901201', end_date='20210101')
    df.reset_index(drop=True)
    df.to_sql(table_name, __engine, index=False, if_exists='append')


if __name__ == '__main__':
    ts_code = config.TEST_TS_CODE_1
    index_code = config.TEST_INDEX_CODE_1
    # init_stock_index(index_code)
    # init_income(ts_code,force=True)
    # init_balancesheet(ts_code, force=True)
    # init_cashflow(ts_code, force=True)
