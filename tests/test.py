import pandas as pd
import numpy as np
from dao.db_pool import get_engine
from sqlalchemy.types import VARCHAR, Integer, DATE, DECIMAL, INT, BIGINT, FLOAT, Float, NUMERIC
import re

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


class Student:
    def __init__(self, name, age):
        self.__name = name
        self.__age = age

    @property
    def name(self):
        print('get name')
        return self.__name

    @name.setter
    def name(self, name):
        print('set name')
        self.__name = name


def match_names(file_path, dict_sheet, like):
    df_d = pd.read_excel(file_path, dict_sheet)
    for i, row in df_d.iterrows():
        desc = row['desc']
        name = row['name']
        r = '.*' + like + '.*'
        o = re.match(r, desc)
        if o:
            print('{}-{}'.format(desc, name))

if __name__ == '__main__':
    like = '金融负债'
    match_names(file_path, dict_sheet, like)
