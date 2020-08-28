import pandas as pd
import numpy as np
from dao.db_pool import get_engine
from sqlalchemy.types import VARCHAR, Integer, DATE, DECIMAL, INT, BIGINT, FLOAT, Float,NUMERIC


# import conf.config as config
# import stock.calc as calc
# import dao.tushare_dao as tsdao


def drop_more_nan_row(df, column_name):
    pass


def test():
    import pandas as pd
    data = [[1, 1, 1, 2, 3, 3], [0.2, 0.3, 0.4, 0.1, 0.2, 0.4]]
    columns = list('abcdef')
    dtype = {"b": FLOAT(), 'a': Float()}
    df = pd.DataFrame(data, columns=columns)
    df.to_sql('test_table', get_engine(), dtype=dtype, index=False, if_exists='replace')

    # df.to_sql('test',get_engine())


if __name__ == '__main__':
    test()
