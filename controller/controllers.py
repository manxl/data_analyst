from conf.config import Y, M, D, OPERATION_TRUNCATE, OPERATION_APPEND, ERROR_NOT_INITED
import abc, logging, time
import pandas as pd
from dao.db_pool import get_engine, get_pro
from web.model.pojo import His
from sqlalchemy.types import VARCHAR, DATE, INT, Float, DECIMAL, Integer
from datetime import date
from tools.utils import df_add_y_m, df_add_y
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import dao.db_dao as dao
from conf.config import *


class BaseController:
    def __init__(self, interface, cyc, operate, biz_code=None, biz_col_name=None):
        logging.debug('Init Controller {}'.format(self.__class__))
        self.interface = interface
        self.biz_code = biz_code
        self.biz_col_name = biz_col_name
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
            # exit(4)
            logging.debug('{} {} is not need process'.format(self.biz_code,self.interface))
            pass

    def is_need_process(self):
        if self.his is None:
            return CTL_PROCESS_INIT

        today = date.today()
        if self.cyc == CTL_CYCLE_YEAR and self.his.y != today.year:
            pass
        elif self.cyc == CTL_CYCLE_MONTH and (self.his.y != today.year or self.his.m != today.month):
            pass
        elif self.cyc == CTL_CYCLE_DAY and (
                self.his.y != today.year or self.his.m != today.month or self.his.d != today.day):
            pass
        else:
            return None
        return CTL_PROCESS_UPDATE

    def load_his(self):
        his = His.query.filter_by(table_name=self.get_table_name(), biz_code=self.biz_code)
        try:
            his = his.one()
        except Exception:
            his = None
        self.his = his

    def init(self):
        self._init_ts()
        self._init_his()

    @abc.abstractmethod
    def _init_ts(self):
        pass

    def _init_his(self):
        his = His(self.get_table_name(), self.biz_code).init()
        his.save()

    def update(self):
        self._update_ts()
        self._update_his()

    def _update_his(self):
        his = His.query.filter_by(table_name=self.get_table_name(), biz_code=self.biz_code).one()
        his.init()
        his.save()

    @abc.abstractmethod
    def _update_ts(self):
        pass

    def delete(self):
        self._delete_ts()
        self._delete_his()

    def _delete_his(self):
        his = His.query.filter_by(table_name=self.get_table_name(), biz_code=self.biz_code).one()
        his.delete()

    def _delete_ts(self):
        if self.operate == CTL_OPERATE_TRUNCATE:
            sql = 'drop table if exists {}'.format(self.get_table_name())
        elif self.operate == CTL_OPERATE_APPEND:
            sql = "delete from {} where {} = '{}'".format(self.get_table_name(), self.biz_col_name, self.biz_code)
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
        sql = 'drop table if exists {}'.format(self.get_table_name() + '_detail')
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


class IndexWeightController(BaseController):

    def __init__(self, biz_code):
        super().__init__('index_weight', CTL_CYCLE_MONTH, CTL_OPERATE_APPEND, biz_code=biz_code,
                         biz_col_name='index_code')

    def _update_ts(self):
        self._delete_ts()
        self._init_ts()

    def _init_ts(self):
        y_start = 1990

        __pool = ThreadPoolExecutor(max_workers=MULTIPLE, thread_name_prefix="test_")
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
            f1 = __pool.submit(get_pro().index_weight, index_code=self.biz_code, start_date=first, end_date=first)
            f2 = __pool.submit(get_pro().index_weight, index_code=self.biz_code, start_date=last, end_date=last)
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

        df.to_sql(self.get_table_name(), get_engine(), dtype=dtype, index=False, if_exists='append')


class FinaBaseController(BaseController):

    def __init__(self, table, biz_code):
        super().__init__(table, CTL_CYCLE_DAY, CTL_OPERATE_APPEND, biz_code=biz_code, biz_col_name='ts_code')

    def _update_ts(self):
        self._init_ts(his=self.his)

    def _init_ts(self, his=None):
        if 'income' == self.interface:
            from dao.ts import Income
            handler = Income(self.biz_code, his)
        elif 'cashflow' == self.interface:
            from dao.ts import CashFlow
            handler = CashFlow(self.biz_code, his)
        elif 'fina_indicator' == self.interface:
            from dao.ts import FinaIndicator
            handler = FinaIndicator(self.biz_code, his)
        elif 'balancesheet' == self.interface:
            from dao.ts import BalanceSheet
            handler = BalanceSheet(self.biz_code, his)
        else:
            raise Exception('unsuppurt interface {}'.format(self.interface))
        handler.process()


class IncomeController(FinaBaseController):
    def __init__(self, biz_code):
        super().__init__('income', biz_code)
