import conf.config as config
import dao.db_dao as dao
import pandas as pd
from time import time
from dao.db_pool import get_engine
# import matplotlib
# matplotlib.use('Agg')
# from matplotlib import pyplot as mp
import matplotlib.pyplot as mp


def test_calc_repay(ts_code, start, end, begin_position=100):
    df = dao.get_dividend(ts_code, start, end)

    inc_cash = 0.0
    inc_stock = begin_position

    df = df[::-1]
    # for row in df.itertuples():
    for i, row in df.iterrows():
        # for row in df.items():
        cash = row['cash_div']
        stk = row['stk_div']
        if cash:
            inc_cash += inc_stock * cash
        if stk:
            inc_stock *= 1 + stk
        # print("i:{}\tstk:{}\tcash:{}\ti_cash:{}\ti_stk:{}".format(i, stk, cash, inc_cash, inc_stock))
        print("{}\t{}\t{}\t{}\t{}".format(i, stk, cash, inc_cash, inc_stock))
    return inc_stock, inc_cash


def test_one_repay(ts_code, start_year, period):
    end_year = period + start_year
    first_begin, first_end = dao.get_trade_date(start_year, 1)
    last = dao.get_trade_date(end_year, 1)[1]
    df_price_1 = dao.get_stock_price_monthly(ts_code, first_end)
    df_price_2 = dao.get_stock_price_monthly(ts_code, last)
    p_open = df_price_1.loc[0]['open']
    p_close = df_price_2.loc[0]['close']

    position_begin = 100

    position, cash = test_calc_repay(ts_code, first_end, last, begin_position=position_begin)

    principal = position_begin * p_open
    value = position * p_close + cash

    earnings = value - principal
    earnings_percent = earnings / principal

    print('first_end:\t', first_end)
    print('last:\t', last)
    print('p_open:\t', p_open)
    print('p_close:\t', p_close)
    print('position:\t', position)
    print('cash:\t', cash)
    print('principal:\t', principal)
    print('value:\t', value)
    print('earnings:\t', earnings)
    print('earnings_percent:\t', earnings_percent * 100)


class Analyser:
    def __init__(self, y, m, earning_duration=None, earning_mean_year=None, earning_inc_ratio=None, underrate_ep_2_3A_multi=None,
                 underrate_divident_2_3A=None, low_percent=None, underrate_price_2_touch_assert=None, underrate_price_2_cur_ass_val=None,
                 ):
        self.__y = y
        self.__m = m
        self.earning_duration = 5 if earning_duration is None else earning_duration
        self.earning_mean_year = 3 if earning_mean_year is None else earning_mean_year
        self.earning_inc_ratio = 1.07 if earning_inc_ratio is None else earning_inc_ratio

        # 1. ep L3A*2
        self.underrate_ep_2_3A_multi = 2 if underrate_ep_2_3A_multi is None else underrate_ep_2_3A_multi
        # 2. dividend > L3A 2/3
        self.underrate_divident_2_3A = 2 / 3 if underrate_divident_2_3A is None else underrate_divident_2_3A
        # 3. pe in lowest 10%
        self.low_percent = 1 if low_percent is None else low_percent
        # 4. price < touch assert 2/3
        self.underrate_price_2_touch_assert = 2 / 3 if underrate_price_2_touch_assert is None else underrate_price_2_touch_assert
        # 5. price < total_cur_assets VALUE 2/3
        self.underrate_price_2_cur_ass_val = 2 / 3 if underrate_price_2_cur_ass_val is None else underrate_price_2_cur_ass_val

        self.stat = {}

        self.__result = None

        self.standard_table_name = 'standard'
        self.standard_start_table_name = 'standard_stat'

    def process(self):
        if self.__load_db():
            print('{} {} load db matrix ok'.format(self.__y, self.__m))
            return
        print('{} {} analyse start'.format(self.__y, self.__m))
        self.__metadata()
        self.exec_mask()

    def __load_db(self):
        stat = dao.get_standard_stat(self.__y, self.__m)
        if stat is not None and len(stat) > 0:
            self.stat = stat
            self.__result = dao.get_standard(self.__y, self.__m)
            return 'ok'
        return None

    def __metadata(self):
        start = time()

        a3 = dao.get_liability(self.__y)

        pe_threshold = dao.get_pe_low(self.__y, self.__m, self.low_percent)
        start_year = self.__y - self.earning_duration - self.earning_mean_year

        df = dao.get_analyse_collection(self.__y, self.__m, pe_threshold, start_year)

        print('=' * 32, 'load cost:', time() - start)
        start = time()
        """
            UNDERRATE
        """

        # 1. ep L3A*2
        self.stat['ep_2_3A'] = df['ep'] * 100 > a3 * self.underrate_ep_2_3A_multi
        # 2. dividend > L3A 2/3
        self.stat['dividend_2_3A'] = df['dv_ratio'] > a3 * self.underrate_divident_2_3A

        # 3. pe in lowest 10%
        # add above

        # 4. price < tangible_asset 2/3
        # self.stat['price_touch_assert'] = (df['total_mv'] * 10000 / self.underrate_price_2_touch_assert) < df['total_assets'] - df['intan_assets'] - df['r_and_d'] - df['goodwill']
        # 'intan_assets'] - df['r_and_d'] - df['goodwill'] - df['lt_amor_exp'] - df['defer_tax_assets']

        self.stat['price_touch_assert_exc_1'] = (df['total_mv'] * 10000 ) /2/3 < df['tangible_asset']
        # self.stat['price_touch_assert_exc_066'] = (df['total_mv'] * 10000 / 0.666 ) < df['total_hldr_eqy_exc_min_int']

        # 5. price < total_cur_assets VALUE 2/3
        # self.stat['price_2_cur_asset_val'] = df['total_mv'] * 10000 / self.underrate_price_2_cur_ass_val < df['total_cur_assets'] - df['total_liab']

        """
            FINANCIAL
        """
        # 1, current_ratio > 2 |  quick_ratio  > 1
        self.stat['cur_quick_ratio'] = (df['current_ratio'] > 2) | (df['quick_ratio'] > 1)

        # 2. debt-to-equity < 1
        # self.stat['debt-to-equity_exc'] = df['total_liab'] < df['total_hldr_eqy_exc_min_int']
        self.stat['debt-to-equity_inc'] = df['total_liab'] < df['total_hldr_eqy_inc_min_int']

        # Current Assets VAL > debt 1/2
        self.stat['cur_asset_val_2_debt'] = df['total_cur_assets'] - df['total_liab'] > df['total_liab'] / 2

        """
        EARNING
        """
        # 1. 10 year earning rate 7%
        # 2. 10 year earning rate de decrease more than 5% below 2year
        print('=' * 32, 'analyse cost:', time() - start)
        start = time()

        detail_list = dao.get_fina(df.ts_code, self.__y - self.earning_duration - self.earning_mean_year, self.__y, self.__m)
        print('=' * 32, 'load cost:', time() - start)
        start = time()
        self.stat['earning_power'] = df['ts_code'].apply(
            lambda x: check_earning_power(x, self.__y, self.earning_duration, self.earning_mean_year, self.__m, self.earning_inc_ratio,
                                          data=detail_list))

        print('=' * 32, 'analyse cost:', time() - start)
        # start = time()

        self.__result = df

    def exec_mask(self):
        df = self.__result
        # total count
        total = len(df)
        r = None
        for k, mask in self.stat.items():
            self.stat[k] = mask.sum() / total
            if r is None:
                r = mask
            else:
                r = r & mask
            print('{}\t:{}'.format(k,r.sum()))
        df = df[r]

        self.stat['total'] = total
        self.stat['all'] = len(df)
        self.stat['all_ratio'] = len(df) / total

        df_stat = pd.DataFrame(self.stat, index=pd.Series([self.__y]))
        df_stat.insert(0, 'm', self.__m)
        df_stat.insert(0, 'y', self.__y)

        df.to_sql(self.standard_table_name, get_engine(), index=False, if_exists='append')
        df_stat.to_sql(self.standard_start_table_name, get_engine(), index=False, if_exists='append')

        self.stat = df_stat
        self.__result = df


def analyse_pe():
    start = 1994
    y = []
    p_10 = []
    p_50 = []
    for i in range(27):
        print(i)
        year = start + i
        y.append(year)

        pe = dao.get_pe_low(year, 12, 0.1)
        p_10.append(pe)

        # pe = dao.get_pe_low(year, 12, 0.5)
        # p_50.append(pe)

    mp.plot(y, p_10, label='10%')
    # mp.plot(y, p_50,label='50%')

    mp.legend()
    mp.show()


def check_earning_power(ts_code, y, earning_duration, earning_mean_year, m, ratio, data=None):
    # print('=' * 32, 'checking', ts_code)
    if data is None:
        df = dao.get_fina(ts_code, y - earning_duration - earning_mean_year, y, m)
    else:
        df = data[data['ts_code'] == ts_code]

    if not len(df) or len(df) < earning_duration + earning_mean_year:
        return False

    try:
        a = df['eps'] > 0
    except KeyError as e:
        print(e)
        exit(4)
    if a.sum() < len(a):
        return False

    e_n = df[:earning_mean_year]['eps'].mean()
    e_f = df[:-1 - earning_mean_year:-1]['eps'].mean()
    r = e_n / e_f
    if r < ratio:
        # print('check earning power exit by ratio - > ts_code:', ts_code)
        # print(df[['y', 'eps']])
        # print(e_n)
        # print(e_f)
        return False

    a_a = df[:earning_duration]
    a_b = df[1:earning_duration + 1]
    a_a = a_a.reset_index(drop=True)
    a_b = a_b.reset_index(drop=True)
    a_c = (a_a['eps'] < a_b['eps'] * 0.95)
    if a_c.sum() > 2:
        # print('check earning power exit by flacturat- > ts_code:', ts_code)
        # print('a:', a_a)
        # print('b:', a_b)
        # print('c:', a_c)
        return False
    return True


def analys_array():
    start, m = 2000, 12
    for i in range(0, 21):
        y = start + i
        if y == 2020:
            m = 6
        a = Analyser(y, m)
        a.process()


def show():
    df = dao.get_stat()
    filters = ['all', 'all_ratio', 'total', 'y', 'm']
    for key in df.columns.values:
        if key not in filters:
            mp.plot(df[key], label=key)

    mp.legend()
    mp.show()


if __name__ == '__main__':
    dao.before_2_clean()
    analys_array()
    # show()
