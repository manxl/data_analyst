import dao.tushare_dao as ts_dao
import dao.db_dao as db
import conf.config as config
from concurrent.futures import ThreadPoolExecutor


def init_matrix_index_needs(index_code):
    ts_codes = db.get_index_distinct_codes(index_code)

    __pool = ThreadPoolExecutor(max_workers=config.MULTIPLE, thread_name_prefix="test_")
    fs = []
    for ts_code in ts_codes['con_code']:
        f = __pool.submit(init_target_stock_base, ts_code)
        fs.append(f)


def init_target_stock_base(ts_code):
    ts_dao.init_stock_price_monthly(ts_code)
    ts_dao.init_income(ts_code)
    ts_dao.init_balancesheet(ts_code)
    ts_dao.init_cashflow(ts_code)
    ts_dao.init_dividend(ts_code)


if __name__ == '__main__':
    # ts_code = '000022.SZ'
    ts_code = config.TEST_TS_CODE_1
    init_target_stock_base(ts_code)
