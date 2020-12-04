import pandas as pd
import datetime
from dao.db_pool import get_engine
from conf.config import *

from analyse.my_data_sql import *


def get_liability(y=None):
    if y is None:
        y = datetime.datetime.now().year
    aaa = pd.read_sql_query(f"select * from liability  where y = {y};", get_engine()).iloc[0, 2]
    return aaa / 100


def get_nincome_roe_pe_meta(ts_code):
    sql_abc = """select 
	s.ts_code,f.y,f.m,f.end_date,
	i.n_income_attr_p ,
	f.roe,
	m.close ,m.pe,m.pe_ttm,
	d.close as c2 ,d.pe p2 ,d.pe_ttm pt2
from 
	stock_basic s,fina_indicator f, daily_basic_month m ,daily_basic d,income i
where 
	s.ts_code = f.ts_code and f.ts_code = m.ts_code and m.ts_code = d.ts_code and d.ts_code = i.ts_code
	and f.y = m.y  and m.y = i.y 
	and f.m = m.m  and m.m = i.m
	and s.ts_code = '{}'
	and f.y > 2010 
order by f.y ,f.m ;""".format(ts_code)
    all_abc = pd.read_sql_query(sql_abc, get_engine())
    df = all_abc[all_abc.m == 3]
    df['yl'] = df['y']
    df = df.set_index(['y'])
    for i in range(1, 5):
        t = all_abc[all_abc.m == 3 * i]
        t = t.set_index(['y'])
        df['n{}'.format(i * 3)] = t['n_income_attr_p']
        df['r{}'.format(i * 3)] = t['roe']
        df['pe{}'.format(i * 3)] = t['pe']
        df['pe_ttm{}'.format(i * 3)] = t['pe_ttm']

    return df


def get_balancesheet_df(ts_code, flag=None, y=None):
    if flag == 'assert':
        if not y:
            sql = balancesheet_get_stock_info_his_by_ts_code.format(**locals())
        else:
            sql = balancesheet_get_stock_industry_info.format(**locals())
    elif flag == 'liab':
        if not y:
            sql = balancesheet_get_stock_info_his_by_ts_code_2.format(**locals())
        else:
            sql = balancesheet_get_stock_industry_info_2.format(**locals())
    else:
        raise AttributeError('flag neither assert nor liab.')

    df = pd.read_sql_query(sql, get_engine())

    if 'y' in df.columns:
        df = df.set_index(['y'])
    elif 'ts_code' in df.columns:
        print('mark')
        df.loc[df[df['ts_code'] == ts_code].index[0], ['ts_code']] = ts_code + '(*)'
        df = df.set_index(['ts_code'])
    else:
        raise AttributeError('Nether y nor ts_code must in dataframe!')

    column_list = df.columns.values.tolist()
    last_col = column_list[len(column_list) - 1]

    for col in df.columns.values.tolist():
        # delete nan
        df[col] = df[col].apply(lambda x: 0 if x != x or x is None else x)
        # calc last_col
        if last_col != col:
            df[last_col] = df[last_col] - df[col]

    sm = df.sum(axis=1)
    flag1 = sm != 0
    flag2 = df[df.columns[len(df.columns) - 1]] != 0
    sm = sm[flag1 & flag2]
    df = df[flag1 & flag2]

    if len(df) == 0:
        return None

    for col in df.columns.values.tolist():
        df[col] = round(df[col] * 100 / sm, 2)

    return df


def kkk():
    sql = """
    select 
        b.y,b.total_hldr_eqy_inc_min_int as e,
        i.n_income as r,
        i.n_income /total_hldr_eqy_exc_min_int as roe,
        d.cash_div_tax*d.base_share as s
    from 
        balancesheet b,income i , fina_indicator f,dividend d
    where 1=1
        and b.ts_code = i.ts_code and i.ts_code = f.ts_code and f.ts_code = d.ts_code
        and b.y = i.y and i.y = f.y and f.y	= d.y
        and b.m = i.m and i.m = f.m
        and b.ts_code = '{}' and b.y between {} and {} and b.m = 12 
        order by b.y asc;
    
    """.format(TEST_TS_CODE_GSYH, 2006, 2010)

    df = pd.read_sql_query(sql, get_engine())
    return df
