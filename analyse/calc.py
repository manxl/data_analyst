import conf.config as config
import dao.db_dao as dao
import matplotlib.pyplot as mp
import pandas as pd
from time import time


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
    def __init__(self, y, m, earning_duration=None, earning_mean_year=None, earning_inc_ratio=None):
        self.__y = y
        self.__m = m
        self.__earning_duration = 5 if earning_duration is None else earning_duration
        self.__earning_mean_year = 3 if earning_mean_year is None else earning_mean_year
        self.__earning_inc_ratio = 1.07 if earning_inc_ratio is None else earning_inc_ratio

        # 1. ep L3A*2
        self.underrate_ep_2_3A_multi = 2
        # 2. dividend > L3A 2/3
        self.underrate_divident_2_3A = 2 / 3
        # 3. pe in lowest 10%
        self.low_percent = 1
        # 4. price < touch assert 2/3
        self.underrate_price_2_touch_assert = 2 / 3
        # 5. price < total_cur_assets VALUE 2/3
        self.underrate_price_2_cur_ass_val = 2 / 3

        self.__stat = {'underrate': {}, 'financial': {}, 'earning': {}}

        self.__result = None

    def process(self):
        self.__base_analyse()
        self.__show()

    def __base_analyse(self):
        start = time()

        a3 = dao.get_liability(self.__y)

        pe_threshold = dao.get_pe_low(self.__y, self.__m, self.low_percent)
        start_year = self.__y - self.__earning_duration - self.__earning_mean_year

        df = dao.get_analyse_collection(self.__y, self.__m, pe_threshold, start_year)

        print('=' * 32, 'load cost:', time() - start)
        start = time()
        """
            UNDERRATE
        """

        # 1. ep L3A*2
        self.__stat['underrate']['ep_2_3A'] = df['ep'] > a3 * self.underrate_ep_2_3A_multi
        # 2. dividend > L3A 2/3
        self.__stat['underrate']['dividend_2_3A'] = df['dv_ratio'] > a3 * self.underrate_divident_2_3A

        # 3. pe in lowest 10%
        # add above

        # 4. price < touch assert 2/3
        self.__stat['underrate']['price_touch_assert'] = (df['total_mv'] / self.underrate_price_2_touch_assert) < df['total_assets'] - df[
            'intan_assets'] - df['r_and_d'] - df['goodwill'] - df['lt_amor_exp'] - df['defer_tax_assets']

        # 5. price < total_cur_assets VALUE 2/3
        self.__stat['underrate']['price_2_cur_asset_val'] = df['total_mv'] / self.underrate_price_2_cur_ass_val < df['total_cur_assets'] - df[
            'total_liab']

        """
            FINANCIAL
        """
        # 1, current_ratio > 2 |  quick_ratio  > 1
        self.__stat['financial']['cur_quick_ratio'] = (df['current_ratio'] > 2) | (df['quick_ratio'] > 1)

        # 2. debt-to-equity < 1
        self.__stat['financial']['debt-to-equity'] = df['total_liab'] < df['total_hldr_eqy_inc_min_int']

        # Current Assets VAL > debt 1/2
        self.__stat['financial']['cur_asset_val_2_debt'] = df['total_cur_assets'] - df['total_liab'] > df['total_liab'] / 2

        """
        EARNING
        """
        # 1. 10 year earning rate 7%
        # 2. 10 year earning rate de decrease more than 5% below 2year
        print('=' * 32, 'analyse cost:', time() - start)
        start = time()

        detail_list = dao.get_fina(df.ts_code, self.__y - self.__earning_duration - self.__earning_mean_year, self.__y, self.__m)
        print('=' * 32, 'load cost:', time() - start)
        start = time()
        self.__stat['earning']['increase_decrease'] = df['ts_code'].apply(
            lambda x: check_earning_power(x, self.__y, self.__earning_duration, self.__earning_mean_year, self.__m, self.__earning_inc_ratio,
                                          data=detail_list))

        print('=' * 32, 'analyse cost:', time() - start)
        # start = time()

    def __show(self):
        for tab, tab_masks in self.__stat.items():
            print('=' * 32, tab)
            for k, mask in tab_masks.items():
                total = len(mask)
                true = mask.sum()
                ratio = true / total
                print('=' * 16, k)

                print('\t\t\ttotal:', total)
                print('\t\t\ttrue:', true)
                print('\t\t\tratio:', ratio)


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
    e_f = df[:-1-earning_mean_year:-1]['eps'].mean()
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


if __name__ == '__main__':
    # test_one_repay(config.TEST_TS_CODE_1, 2010, 10)
    y, m = 2019, 12
    a = Analyser(y, m)
    a.process()
