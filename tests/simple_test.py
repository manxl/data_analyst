import pandas as pd
import numpy as np


def test_method1():
    df = pd.DataFrame({'name': ['yang', 'jian', 'yj'], 'age': [23, 34, 22], 'gender': ['male', 'male', 'female']})
    print(df)
    print(df.columns)
    column_name = 'age'
    location = np.where(df.columns.values.T == column_name)[0][0]
    c = df['gender'].apply(lambda x: '男' if 'male' == x else '女')
    r = df.insert(int(location) + 1, '性别', c)
    print(r)
    print(df)


def test_method_2():
    from dao.db_pool import get_pro
    df = get_pro().query('balancesheet', ts_code='002027.SZ')
    print(df)


if __name__ == '__main__':
    a = [[1, 2, 3], [4, 5, 6, 6]]
    a = [1, 2, 3]
    print(dir(a))
    print(a)
    n = np.array(a)
    print(n.shape)
