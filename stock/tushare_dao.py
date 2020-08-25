import datetime
import pandas as pd
import numpy as np
import tushare as ts
from sqlalchemy.types import VARCHAR, DATE, INT, Integer
from sqlalchemy import create_engine

db_name = 'analyst'


def get_engine():
    return create_engine("mysql+pymysql://manxl:111@127.0.0.1:3306/{}?charset=utf8".format(db_name), pool_size=20)


pro = ts.pro_api()


class TuShareDao:

    @staticmethod
    def get_stock_list_all():
        fileds = 'ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs'
        d_l = pro.stock_basic(exchange='', list_status='L', fields=fileds)
        print('L', len(d_l))
        d_d = pro.stock_basic(exchange='', list_status='D', fields=fileds)
        print('D', len(d_d))
        d_p = pro.stock_basic(exchange='', list_status='P', fields=fileds)
        print('P', len(d_p))
        data = d_l.append(d_d).append(d_p)
        print('all size:', len(data))
        data.to_sql('stock_list', get_engine(), schema=db_name)

    @staticmethod
    def get_stock_cf():
        # df = pro.index_basic(ts_code='000016.SH')
        start = '20180905'
        # df = pro.index_weight(index_code='399300.SZ',
        #                       start_date=start, end_date='20180930')
        df = pro.index_weight(index_code='399300.SZ')

        print(df)
        print(len(df))
        d = datetime.datetime.strptime(start, '%Y%m%d')
        w = d.weekday()
        print(w)

    @staticmethod
    def get_trade_dates():
        template_start = '{}00101'
        template_end = '{}91231'
        data = None
        for i in range(4):
            print(i)
            t = 199 + i
            start, end = template_start.format(t), template_end.format(t)
            df = pro.query('trade_cal', start_date=start, end_date=end)
            if data is not None:
                data = data.append(df, ignore_index=True)
            else:
                data = df
            print('start:{},date:{}'.format(start, len(data)))

        # data.to_sql('trade_date_o', get_engine(), if_exists='replace', schema=db_name)
        df = data
        df['y'] = df['cal_date'].apply(lambda s: int(s[:4]))
        df['m'] = df['cal_date'].apply(lambda s: int(s[4:6]))
        # df['cal_date'] = df['cal_date'].astype('M8[D]')
        # df.reset_index(drop=True)

        df.set_index(['y', 'm', 'cal_date'])
        df = df[df['is_open'] == 1]
        df = df.reindex(columns=['y', 'm', 'cal_date', 'is_open', 'exchange'])
        df.to_sql('trade_date_detail',
                  get_engine(),
                  index=False,
                  dtype={'cal_date': DATE(), 'y': Integer(), 'm': INT(), 'is_open': INT(), 'exchange': VARCHAR(8)},
                  # dtype={'cal_date': 'M8[d]'},
                  if_exists='replace', schema=db_name)
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
        r.to_sql('trade_date', get_engine(),
                 index=False,
                 dtype={'start': DATE(), 'end': DATE(), 'y': Integer(), 'm': INT()}
                 , if_exists='replace', schema=db_name)

    @staticmethod
    def get_divid():
        # df = pro.dividend(ts_code='600036.SH', fields='ts_code,div_proc,stk_div,record_date,ex_date')
        # df = pro.dividend(ts_code='600036.SH')
        df = pro.dividend(ts_code='002230.SZ')
        print(df)
        df.to_sql('stock_divide', get_engine(), schema=db_name, if_exists='append')


if __name__ == '__main__':
    # TuShareDao.get_stock_cf()
    # TuShareDao.get_stock_list_all()
    TuShareDao.get_trade_dates()
    # TuShareDao.get_divid()
