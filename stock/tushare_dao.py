import datetime

import tushare as ts
from sqlalchemy import create_engine


def get_engine():
    return create_engine("mysql+pymysql://manxl:111@localhost:3306/analyse?charset=utf8")


pro = ts.pro_api()


class TuShareDao:

    @staticmethod
    def get_stock_list_all():
        """
        is_hs	    str	N	是否沪深港通标的，N否 H沪股通 S深股通
        list_status	str	N	上市状态： L上市 D退市 P暂停上市，默认L
        exchange	str	N	交易所 SSE上交所 SZSE深交所 HKEX港交所(未上线)
        """
        fileds = 'ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs'
        d_l = pro.stock_basic(exchange='', list_status='L', fields=fileds)
        print('L', len(d_l))
        d_d = pro.stock_basic(exchange='', list_status='D', fields=fileds)
        print('D', len(d_d))
        d_p = pro.stock_basic(exchange='', list_status='P', fields=fileds)
        print('P', len(d_p))
        data = d_l.append(d_d).append(d_p)
        print('all size:', len(data))
        data.to_sql('stock_list', get_engine(), schema='analyse')

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
        df = pro.query('trade_cal', start_date='20180101', end_date='20181231')
        df.to_sql('trade_date', get_engine(), schema='analyse')

    @staticmethod
    def get_divid():
        # df = pro.dividend(ts_code='600036.SH', fields='ts_code,div_proc,stk_div,record_date,ex_date')
        # df = pro.dividend(ts_code='600036.SH')
        df = pro.dividend(ts_code='002230.SZ')
        print(df)
        df.to_sql('stock_divide', get_engine(), schema='analyse', if_exists='append')


if __name__ == '__main__':
    # TuShareDao.get_stock_cf()
    # TuShareDao.get_stock_list_all()
    # TuShareDao.get_trade_dates()
    TuShareDao.get_divid()
