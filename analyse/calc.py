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




def base_analyse(y, m, e_duration, e_evg_years, e_ratio):
    start = time()

    low_percent = 1
    a3 = dao.get_liability(y)

    pe_threshold = dao.get_pe_low(y, m, low_percent)
    start_year = y - e_duration - e_evg_years
    df = dao.get_analyse_collection(y, m, pe_threshold, start_year)

    print('=' * 32, 'load cost:', time() - start)
    start = time()
    """
        UNDERRATE
    """
    # 1. ep L3A*2
    mask_UNDERRATE_ep = df['ep'] > a3 *2
    # df = df[mask_ep]
    stat_count_filtration(df, mask_UNDERRATE_ep, 'mask_UNDERRATE_ep')

    # 2. dividend > L3A 2/3
    mask_UNDERRATE_divident = df['dv_ratio'] > a3 / 3 * 2
    stat_count_filtration(df, mask_UNDERRATE_divident, 'mask_UNDERRATE_divident')

    # 3. pe in lowest 10%
    # add above

    # 4. price < youxiangzichanczhamgmianjiazhi 2/3
    # total_assets - (intan_assets  r_and_d goodwill lt_amor_exp defer_tax_assets)
    price_UNDERRATE_real_mask = (df['total_mv'] * 1.5) < df['total_assets'] - df['intan_assets'] - df['r_and_d'] - df[
        'goodwill'] - df['lt_amor_exp'] - df['defer_tax_assets']
    stat_count_filtration(df, price_UNDERRATE_real_mask, 'price_UNDERRATE_real_mask')

    # 5. price < total_cur_assets VALUE 2/3
    ####   edit
    mask_UNDERRATE_cur_ass_val = df['total_mv'] * 3 / 2 < df['total_cur_assets'] - df['total_liab']
    stat_count_filtration(df, mask_UNDERRATE_cur_ass_val, 'mask_UNDERRATE_cur_ass_val')

    """
        FINANCIAL
    """
    # 1, current_ratio > 2 |  quick_ratio  > 1
    mask_FINANCIAL_ratio = (df['current_ratio'] > 2) | (df['quick_ratio'] > 1)
    stat_count_filtration(df, mask_FINANCIAL_ratio, 'mask_FINANCIAL_ratio')

    # 2. debt-to-equity < 1
    mask_FINANCIAL_liability = df['total_liab'] < df['total_hldr_eqy_inc_min_int']
    # df = df[df['total_ncl'] < df['total_hldr_eqy_inc_min_int']]
    stat_count_filtration(df, mask_FINANCIAL_liability, 'mask_FINANCIAL_liability')

    # Current Assets VAL > debt 1/2
    mask_FINANCIAL_asset_VAL = df['total_cur_assets'] - df['total_liab'] > df['total_liab'] / 2
    stat_count_filtration(df, mask_FINANCIAL_asset_VAL, 'mask_FINANCIAL_asset_VAL')
    """
    EARNING POWER
    """
    # 1. 10 year earning rate 7%
    # 2. 10 year earning rate de decrease more than 5% below 2year
    print('=' * 32, 'analyse cost:', time() - start)
    start = time()
    detail_list = dao.get_fina(df.ts_code, y - e_duration - e_evg_years, y, m)
    print('=' * 32, 'load cost:', time() - start)
    start = time()
    earning_power_mask = df['ts_code'].apply(
        lambda x: check_earning_power(x, y, e_duration, e_evg_years, m, e_ratio, data=detail_list))
    # df = df[earning_power_mask]
    stat_count_filtration(df, earning_power_mask, 'earning_power_mask')

    print('=' * 32, 'analyse cost:', time() - start)
    start = time()

    # print('=' * 32, 'RESULT LENGTH:', len(df))
    # print('=' * 32, 'RESULT DATA:', df)

    show()


def show():
    for k, v in stat.items():
        print(k)
        print('\ttotle:', v['totle'])
        print('\tfiltration:', v['filtration'])
        print('\tper:', v['filtration'] / v['totle'])


stat = {}


def stat_count_filtration(df, mask, name):
    if name not in stat:
        stat[name] = {'totle': len(mask), 'filtration': mask.sum()}
    # df = df[mask]


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


def check_earning_power(ts_code, y, e_dur, e_evg, m, ratio, data=None):
    # print('=' * 32, 'checking', ts_code)
    if data is None:
        df = dao.get_fina(ts_code, y - e_dur - e_evg, y, m)
    else:
        df = data[data['ts_code'] == ts_code]

    if not len(df) or len(df) < e_dur + e_evg:
        return False

    try:
        a = df['eps'] > 0
    except KeyError as e:
        print(e)
        exit(4)
    if a.sum() < len(a):
        return False

    e_n = df[:3]['eps'].mean()
    e_f = df[:-4:-1]['eps'].mean()
    r = e_n / e_f
    if r < ratio:
        # print('check earning power exit by ratio - > ts_code:', ts_code)
        # print(df[['y', 'eps']])
        # print(e_n)
        # print(e_f)
        return False

    a_a = df[:e_dur]
    a_b = df[1:e_dur + 1]
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
    y, m, e_duration, e_evg_years, e_ratio = 2019, 12, 5, 3, 1.07
    base_analyse(y, m, e_duration, e_evg_years, e_ratio)
