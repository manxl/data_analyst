import pandas as pd
import numpy as np
from sqlalchemy.types import VARCHAR, DATE, INT, Integer
import conf.config as config
import stock.calc as calc
import dao.tushare_dao as tsdao

if __name__ == '__main__':
    ts_code = config.TEST_TS_CODE_1
    # Test.test_date()
    # Test.test_info()
    tsdao.init_stock_monthly(ts_code)
    calc.test_calc_repay(ts_code)
    # tsdao.init_dividend(ts_code, force=True)
