from conf.config import Y, M, D, OPERATION_TRUNCATE, OPERATION_APPEND, ERROR_NOT_INITED
import pandas as pd
from dao.db_pool import get_engine, get_pro
import abc, logging
from web.model.pojo import His
from sqlalchemy.types import VARCHAR, DATE, INT, Float, DECIMAL, Integer
from datetime import date
from dao.db_pool import MySQL
from tools.utils import df_add_y_m, df_add_y
import numpy as np

CTL_CYCLE_YEAR = 'y'
CTL_CYCLE_MONTH = 'm'
CTL_CYCLE_DAY = 'd'
CTL_OPERATE_TRUNCATE = 'T'
CTL_OPERATE_APPEND = 'A'


class BaseController:
    def __init__(self, interface, cyc, operate, key=None, key_name=None):
        logging.debug('Init Controller {}'.format(self.__class__))
        self.interface = interface
        self.key = key
        self.key_name = key_name
        self.cyc = cyc
        self.operate = operate
        self.his = None

        self.load_his()

    def get_table_name(self):
        return self.interface

    def process(self):
        if self.his is None:
            self.init()
        elif type(self.his) is His and self.is_need_process():
            self.update()
        else:
            exit(4)

    def is_need_process(self):
        if self.his is None:
            return True

        today = date.today()
        if self.cyc == CTL_CYCLE_YEAR and self.his.y != today.year:
            return True
        elif self.cyc == CTL_CYCLE_MONTH and (self.his.y != today.year or self.his.m != today.month):
            return True
        elif self.cyc == CTL_CYCLE_DAY and (
                self.his.y != today.year or self.his.m != today.month or self.his.d != today.day):
            return True

    def load_his(self):
        his = His.query.filter_by(table_name=self.get_table_name(), key=self.key)
        try:
            his = his.one()
        except Exception as e:
            his = None
        self.his = his

    def init(self):
        self._init_his()
        self._init_ts()

    def _init_his(self):
        his = His(self.get_table_name(), self.key).init()
        his.add_update()

    @abc.abstractmethod
    def _init_ts(self):
        pass

    def update(self):
        self._update_his()
        self._update_ts()

    def _update_his(self):
        his = His.query.filter_by(table_name=self.get_table_name(), key=self.key).one()
        his.init()
        his.add_update()

    @abc.abstractmethod
    def _update_ts(self):
        pass

    def delete(self):
        self._delete_his()
        self._delete_ts()

    def _delete_his(self):
        his = His.query.filter_by(table_name=self.get_table_name(), key=self.key).one()
        his.delete()

    def _delete_ts(self):
        if self.operate == CTL_OPERATE_TRUNCATE:
            sql = 'drop table if exists {}'.format(self.get_table_name())
        elif self.operate == CTL_OPERATE_APPEND:
            sql = "delete from {} where {} = '{}'".format(self.get_table_name(), self.key_name, self.key)
        get_engine().execute(sql)
        logging.info(sql)


class StockBasicController(BaseController):

    def __init__(self):
        super().__init__('stock_basic', CTL_CYCLE_YEAR, CTL_OPERATE_TRUNCATE)

    def _init_ts(self):
        print('start init list...')
        fileds = 'ts_code,symbol,name,area,industry,fullname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs'
        d_l = get_pro().query(self.interface, exchange='', list_status='L', fields=fileds)
        print('L', len(d_l))
        d_d = get_pro().query(self.interface, exchange='', list_status='D', fields=fileds)
        print('D', len(d_d))
        d_p = get_pro().query(self.interface, exchange='', list_status='P', fields=fileds)
        print('P', len(d_p))
        df = d_l.append(d_d).append(d_p)
        print('all size:', len(df))

        dtype = {'ts_code': VARCHAR(length=10), 'symbol': VARCHAR(length=8), 'name': VARCHAR(length=20),
                 'area': VARCHAR(length=10), 'industry': VARCHAR(length=32), 'fullname': VARCHAR(length=32),
                 'market': VARCHAR(length=10), 'exchange': VARCHAR(length=10), 'curr_type': VARCHAR(length=5),
                 'list_status': VARCHAR(length=1), 'list_date': DATE(), 'delist_date': DATE(),
                 'is_hs': VARCHAR(length=1)}

        df.to_sql(self.get_table_name(), get_engine(), dtype=dtype, index=False, if_exists='replace')

    def _update_ts(self):
        self._init_ts()


class TradeCalController(BaseController):

    def __init__(self):
        super().__init__('trade_cal', CTL_CYCLE_YEAR, CTL_OPERATE_TRUNCATE)

    def _update_ts(self):
        self._delete_ts()

    def _delete_ts(self):
        sql = 'drop table if exists {}'.format(self.get_table_name()+'_detail')
        get_engine().execute(sql)
        logging.info(sql)
        sql = 'drop table if exists {}'.format(self.get_table_name())
        get_engine().execute(sql)
        logging.info(sql)

    def _init_ts(self):
        template_start = '{}00101'
        template_end = '{}91231'
        data = None
        for i in range(4):
            print(i)
            t = 199 + i
            start, end = template_start.format(t), template_end.format(t)
            df = get_pro().query(self.interface, start_date=start, end_date=end)
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
        df.to_sql(self.get_table_name() + '_detail',
                  get_engine(),
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
        r.to_sql(self.get_table_name(), get_engine(),
                 index=False,
                 dtype={'first': DATE(), 'last': DATE(), 'y': Integer(), 'm': INT()}
                 , if_exists='replace')
