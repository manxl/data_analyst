import numpy as np
import pandas as pd
import tushare as ts
from sqlalchemy.types import VARCHAR, Float, Integer, DATE, DECIMAL, INT, BIGINT
import conf.config as config
import dao.db_pool as pool

__pro = ts.pro_api()
__engine = pool.get_engine()


def df_add_y_m(df, column_name):
    df['y'] = df[column_name].apply(lambda s: int(s[:4]))
    df['m'] = df[column_name].apply(lambda s: int(s[4:6]))


def get_stock_list_all():
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


def get_stock_cf():
    # df = pro.index_basic(ts_code='000016.SH')
    start = '20180905'
    # df = pro.index_weight(index_code='399300.SZ',
    #                       start_date=start, end_date='20180930')
    df = __pro.index_weight(index_code='399300.SZ')

    df_add_y_m(df, 'trade_date')

    dtype = {'index_code': VARCHAR(length=10), 'con_code': VARCHAR(length=10), 'y': INT, 'm': INT,
             'trade_daet': DATE(), 'weight': DECIMAL(precision=10, scale=6)}

    df = df.reindex(columns='index_code,con_code,y,m,trade_date,weight'.split(','))
    # df.reset_index(drop=True)
    # df = df.reindex(columns='ts_code,end_date,ex_date,div_proc,stk_div,cash_div'.split(','))
    df.to_sql('index_weight', __engine, dtype=dtype, index=False, if_exists='replace')


def get_trade_dates():
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
             dtype={'start': DATE(), 'end': DATE(), 'y': Integer(), 'm': INT()}
             , if_exists='replace')


def init_stock_monthly(ts_code, force=False):
    # TODO
    print('{} {}'.format(__file__, __engine))
    table_name = 'stock_price_monthly'

    if not need_pull_check(ts_code, table_name, force):
        return

    df = __pro.monthly(ts_code=ts_code, fields='ts_code,trade_date,open,high,low,close,vol,amount')
    df_add_y_m(df, 'trade_date')
    dtype = {'ts_code': VARCHAR(length=10), 'trade_date': DATE(), 'y': INT, 'm': INT,
             'open': DECIMAL(precision=8, scale=2), 'high': DECIMAL(precision=8, scale=2),
             'low': DECIMAL(precision=8, scale=2), 'close': DECIMAL(precision=8, scale=2),
             'vol': BIGINT(), 'amount': BIGINT()}
    df.to_sql(table_name, __engine, dtype=dtype, index=False, if_exists='append')


def init_dividend(ts_code, force=False):
    table_name = 'stock_dividend'

    if not need_pull_check(ts_code, table_name, force):
        print('need not 2 pull {}'.format(ts_code))
        return

    print('start 2 pull {} dividend .'.format(ts_code))
    df = __pro.dividend(ts_code=ts_code, fields='ts_code,end_date,div_proc,stk_div,cash_div,ex_date')
    df = df[df['div_proc'].str.contains('实施')]
    df.reset_index(drop=True)
    df = df.reindex(columns='ts_code,end_date,ex_date,div_proc,stk_div,cash_div'.split(','))
    dtype = {'ts_code': VARCHAR(length=10), 'end_date': DATE(), 'div_proc': VARCHAR(length=10),
             'stk_div': DECIMAL(precision=10, scale=8), 'cash_div': DECIMAL(precision=12, scale=8),
             'ex_date': DATE()}

    df.to_sql(table_name, __engine, dtype=dtype, index=False, if_exists='append')


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


if __name__ == '__main__':
    ts_code = config.TEST_TS_CODE_1
    # TuShareDao.get_stock_list_all()
    # TuShareDao.get_stock_cf()
    # TuShareDao.get_trade_dates()
    init_stock_monthly(ts_code)
    # TuShareDao.init_dividend(ts_code)
    # TuShareDao.init_dividend('002230.SZ')
    # test_calc_repay()
