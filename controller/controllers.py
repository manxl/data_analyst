from abc import ABC

from conf.config import Y, M, D, OPERATION_TRUNCATE, OPERATION_APPEND, ERROR_NOT_INITED
import abc, logging, time
import pandas as pd
from dao.db_pool import get_engine, get_pro
from web.model.pojo import His
from sqlalchemy.types import VARCHAR, DATE, INT, Float, DECIMAL, Integer, FLOAT
from datetime import date
from tools.utils import df_add_y_m, df_add_y
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import dao.db_dao as dao
from conf.config import *


class BaseController:

    def __init__(self, interface, cyc, operate, biz_code=None, biz_col_name=None, calc_table=None):
        logging.debug('Init Controller {}'.format(self.__class__))
        self.interface = interface
        self.biz_code = biz_code
        self.biz_col_name = biz_col_name
        self.cyc = cyc
        self.operate = operate
        self.his = None
        self.calc_table = calc_table
        self.load_his()
        self.regedit()

    def regedit(self):
        if self.interface in CTL_INTERFACE_CLASS_MAPPING:
            return
        CTL_INTERFACE_CLASS_MAPPING[self.interface] = self.__class__

    def get_table_name(self):
        return self.interface

    def process(self):
        if self.his is None:
            self.init()
        elif type(self.his) is His and self.is_need_process():
            self.update()
        else:
            # exit(4)
            logging.debug('{} {} is not need process'.format(self.biz_code, self.interface))

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
        his = His.query.filter_by(table_name=self.interface, biz_code=self.biz_code)
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
        his = His(self.interface, self.biz_code).init()
        his.save()

    def update(self):
        # TODO 非普适性逻辑考虑优化
        if self.operate == CTL_OPERATE_TRUNCATE:
            self._delete_ts()

        self._update_ts()

        self._update_his()

    def _update_his(self):
        his = His.query.filter_by(table_name=self.interface, biz_code=self.biz_code).one()
        his.init()
        his.save()

    @abc.abstractmethod
    def _update_ts(self):
        pass

    def delete(self):
        self._delete_ts()
        self._delete_his()

    def _delete_his(self):
        his = His.query.filter_by(table_name=self.interface, biz_code=self.biz_code).one()
        his.delete()

    def _delete_ts(self):
        if self.operate == CTL_OPERATE_TRUNCATE:
            sql = 'drop table if exists {}'.format(self.get_table_name())
        elif self.operate == CTL_OPERATE_APPEND:
            sql = "delete from {} where {} = '{}'".format(self.get_table_name(), self.biz_col_name, self.biz_code)
        get_engine().execute(sql)
        logging.info('==delete_ts ' + sql)
        if self.calc_table:
            if self.operate == CTL_OPERATE_TRUNCATE:
                sql = 'drop table if exists {}'.format(self.get_table_name() + self.calc_table)
            elif self.operate == CTL_OPERATE_APPEND:
                sql = "delete from {} where {} = '{}'".format(self.get_table_name() + self.calc_table,
                                                              self.biz_col_name, self.biz_code)
            get_engine().execute(sql)
            logging.info('==delete_ts ' + sql)


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
        super().__init__('trade_cal', CTL_CYCLE_YEAR, CTL_OPERATE_TRUNCATE, calc_table='_detail')

    def _update_ts(self):
        self._delete_ts()

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
        r = r.data(columns=['y', 'm', 'first', 'last'])
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


class MyIndexController(BaseController):

    def __init__(self, biz_code):
        super().__init__('my_index', CTL_CYCLE_MONTH, CTL_OPERATE_APPEND, biz_code=biz_code, biz_col_name='index_code')

    def get_table_name(self):
        return 'index_weight'

    def _update_ts(self):
        self._delete_ts()
        self._init_ts()

    def _init_ts(self):
        pass
        if self.biz_code == 'tangchao':
            con_codes = '600519.SH,002304.SZ,002415.SZ,002027.SZ,000596.SZ'.split(',')
        elif self.biz_code == 'manxl':
            con_codes = '600519.SH,002304.SZ,002415.SZ,002027.SZ,000596.SZ'.split(',')

        sql_template = "INSERT INTO index_weight VALUES ('{}', '{}', {}, {}, '{}-{}-31', 0 );"

        sqls = []
        for con_code in con_codes:
            sqls.append(sql_template.format(self.biz_code, con_code, 2020, 12, 2020, 12))

        for sql in sqls:
            get_engine().execute(sql)


class FinaBaseController(BaseController):

    def __init__(self, table, biz_code):
        if 'dividend' == table:
            super().__init__(table, CTL_CYCLE_DAY, CTL_OPERATE_APPEND, biz_code=biz_code, biz_col_name='ts_code',
                             calc_table='_stat')
        else:
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
        elif 'dividend' == self.interface:
            from dao.ts import Dividend
            handler = Dividend(self.biz_code, his)
        else:
            raise Exception('unsuppurt interface {}'.format(self.interface))
        try:
            handler.process()
        except Exception as e:
            logging.error(e)


class IncomeController(FinaBaseController):
    def __init__(self, biz_code):
        super().__init__('income', biz_code)


class MultiCtlController(BaseController, ABC):
    def __init__(self, interface, biz_code):
        super().__init__(interface, CTL_CYCLE_DAY, CTL_OPERATE_APPEND, biz_code=biz_code, biz_col_name='biz_code')

    def get_table_name(self):
        return 'his'

    def _update_ts(self):
        self._init_ts()

    @abc.abstractmethod
    def get_biz_data(self):
        pass


class OneFinaController(MultiCtlController):
    def __init__(self, biz_code):
        super().__init__('one_fina', biz_code)
        self._targets = 'balancesheet,income,cashflow,fina_indicator,dividend'.split(',')

    def _init_ts(self, his=None):
        for target in self._targets:
            FinaBaseController(target, biz_code=self.biz_code).process()

    def _delete_ts(self):
        for target in self._targets:
            ctl = FinaBaseController(target, biz_code=self.biz_code)
            if ctl.his is not None:
                ctl.delete()

    def get_biz_data(self):
        sub_ctls = []
        for target in self._targets:
            sub_ctls.append(FinaBaseController(target, biz_code=self.biz_code))
        return sub_ctls


class OneIndexController(MultiCtlController):
    def __init__(self, biz_code):
        super().__init__('one_index', biz_code)

    def _init_ts(self, his=None):
        for ts_code in self._get_con_codes():
            OneFinaController(ts_code).process()

    def _delete_ts(self):
        for ts_code in self._get_con_codes():
            OneFinaController(ts_code).delete()

    def _get_con_codes(self):
        sql = """select * from index_weight where index_code = '{}'
                and trade_date = (select max(trade_date) from  index_weight where index_code = '{}')""".format(
            self.biz_code, self.biz_code, )

        df = pd.read_sql_query(sql, get_engine())
        if len(df) == 0:
            raise Exception('index not initialed.')
        return df['con_code']

    def get_biz_data(self):
        con_codes = self._get_con_codes()
        sub_ctls = []
        for ts_code in con_codes:
            sub_ctls.append(OneFinaController(ts_code))
        return sub_ctls


class AllController(MultiCtlController):
    def __init__(self):
        super().__init__('all', None)

    def _init_ts(self, his=None):
        for ts_code in self._get_con_codes():
            OneFinaController(ts_code).process()

    def delete(self):
        sqls = """delete from his where TABLE_name in ('balancesheet','income','cashflow','fina_indicator','dividend','one_fina','one_index','all');
truncate table balancesheet;
truncate table income;
truncate table cashflow;
truncate table fina_indicator;
truncate table dividend;
truncate table dividend_stat;""".split('\n')
        for sql in sqls:
            get_engine().execute(sql)

    def _delete_ts(self):
        for ts_code in self._get_con_codes():
            OneFinaController(ts_code).delete()
            ctl = OneFinaController(ts_code).delete()
            if ctl.his is not None:
                ctl.delete()

    def _get_con_codes(self):
        sql = """select * from stock_basic  ;"""  # where ts_code = '300519.SZ'

        df = pd.read_sql_query(sql, get_engine())

        return df['ts_code']

    def get_biz_data(self):
        con_codes = self._get_con_codes()
        sub_ctls = []
        for ts_code in con_codes:
            sub_ctls.append(OneFinaController(ts_code))
        return sub_ctls


class DailyBasicMonthController(BaseController):

    def __init__(self):
        super().__init__('daily_basic_month', CTL_CYCLE_DAY, CTL_OPERATE_TRUNCATE)

    # def _get_table_name__(self):
    #     return self.interface + "_month"

    def _init_ts(self):
        self._update_ts()

    def _update_ts(self):
        sql = 'select * from trade_cal where m != 0 ;'
        yms = pd.read_sql_query(sql, get_engine())

        df = None
        for i, row in yms.iterrows():
            first_trade_date_str = row['first'].strftime('%Y%m%d')
            last_last_date_str = row['last'].strftime('%Y%m%d')
            data = get_pro().daily_basic(ts_code='', trade_date=last_last_date_str)
            print(last_last_date_str)
            if df is None:
                df = data
            else:
                df = df.append(data)
        df_add_y_m(df, 'trade_date')
        df.reset_index(drop=True)
        df = df.iloc[::-1]
        dtype = {'ts_code': VARCHAR(length=10), 'trade_date': DATE(), 'close': FLOAT(),
                 'y': INT(), 'm': INT(),
                 'turnover_rate': FLOAT(), 'turnover_rate_f': FLOAT(), 'volume_ratio': FLOAT(),
                 'pe': FLOAT(), 'pe_ttm': FLOAT(), 'pb': FLOAT(),
                 'ps': FLOAT(), 'ps_ttm': FLOAT(), 'dv_ratio': FLOAT(),
                 'dv_ttm': FLOAT(), 'total_share': FLOAT(), 'float_share': FLOAT(),
                 'free_share': FLOAT(), 'total_mv': FLOAT(), 'circ_mv': FLOAT()}
        df.to_sql(self.get_table_name(), get_engine(), dtype=dtype, index=False, if_exists='replace')


class DailyBasicController(BaseController):

    def __init__(self):
        super().__init__('daily_basic', CTL_CYCLE_DAY, CTL_OPERATE_TRUNCATE)

    def _init_ts(self):
        self._update_ts()

    def _update_ts(self):
        cal_date = self._get_nearest_cal_date()
        if cal_date is None:
            return
        trade_date = cal_date.strftime('%Y%m%d')
        df = get_pro().daily_basic(ts_code='', trade_date=trade_date)

        dtype = {'ts_code': VARCHAR(length=10), 'trade_date': DATE(), 'close': FLOAT(),
                 'y': INT(), 'm': INT(),
                 'turnover_rate': FLOAT(), 'turnover_rate_f': FLOAT(), 'volume_ratio': FLOAT(),
                 'pe': FLOAT(), 'pe_ttm': FLOAT(), 'pb': FLOAT(),
                 'ps': FLOAT(), 'ps_ttm': FLOAT(), 'dv_ratio': FLOAT(),
                 'dv_ttm': FLOAT(), 'total_share': FLOAT(), 'float_share': FLOAT(),
                 'free_share': FLOAT(), 'total_mv': FLOAT(), 'circ_mv': FLOAT()}
        df.to_sql(self.get_table_name(), get_engine(), dtype=dtype, index=False, if_exists='append')

    def _get_nearest_cal_date(self):
        sql = 'select cal_date from trade_cal_detail  where cal_date  <= curdate() order by cal_date desc limit 1;'
        df = pd.read_sql_query(sql, get_engine())
        cal_date = df.iloc[0]['cal_date']
        if self.his is not None and cal_date <= self.his.end_date:
            return None
        return cal_date
