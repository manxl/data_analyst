import conf.config as config
import dao.db_dao as db


def test_calc_repay(ts_code):
    df = db.get_dividend(ts_code, '2001-01-01', '2019-01-01')
    init_stock = 100
    inc_cash = 0.0
    inc_stock = init_stock

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