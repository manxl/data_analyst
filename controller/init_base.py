import dao.tushare_dao as ts_dao
import dao.db_dao as db
import conf.config as config
from concurrent.futures import ThreadPoolExecutor


def init_base():
    # init list
    ts_dao.init_stock_list_all()
    # init trade_date
    ts_dao.init_trade_date()
    # init index
    ts_dao.init_stock_index(config.TEST_INDEX_CODE_1)
    # init index stock reports
    init_matrix_index_needs(config.TEST_INDEX_CODE_1)


def init_matrix_index_needs(index_code):
    ts_codes = db.get_index_distinct_codes(index_code)

    __pool = ThreadPoolExecutor(max_workers=config.MULTIPLE, thread_name_prefix="test_")
    fs = []
    for ts_code in ts_codes['con_code']:
        f = __pool.submit(init_target_stock_base, ts_code)
        fs.append(f)


def init_stock_all(limit=None):
    list_all = db.get_list_all(limit)
    __pool = ThreadPoolExecutor(max_workers=config.MULTIPLE, thread_name_prefix="test_")
    fs = []
    for ts_code in list_all['ts_code']:
        f = __pool.submit(init_target_stock_base, ts_code)
        fs.append(f)


def init_target_stock_base(ts_code, force=None):
    ts_dao.init_stock_price_monthly(ts_code, force)
    ts_dao.init_income(ts_code, force)
    ts_dao.init_balancesheet(ts_code, force)
    ts_dao.init_cashflow(ts_code, force)
    ts_dao.init_dividend(ts_code, force)
    ts_dao.init_fina_indicator(ts_code, force)


if __name__ == '__main__':
    # ts_code = '000022.SZ'
    # ts_code = config.TEST_TS_CODE_2
    # init_target_stock_base('601318.SH')
    # init_matrix_index_needs(config.TEST_INDEX_CODE_1)
    # init_base()
    init_stock_all(10)
    # ts_dao.init_dividend(config.TEST_TS_CODE_3, force='drop')

    pass