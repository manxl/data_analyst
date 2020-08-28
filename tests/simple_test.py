import pandas as pd
import numpy as np

if __name__ == '__main__':
    df = pd.DataFrame({'name': ['yang', 'jian', 'yj'], 'age': [23, 34, 22], 'gender': ['male', 'male', 'female']})
    print(df)
    print(df.columns)
    column_name = 'age'
    location = np.where(df.columns.values.T == column_name)[0][0]
    c = df['gender'].apply(lambda x: '男' if 'male' == x else '女')
    r = df.insert(int(location)+1, '性别', c)
    print(r)
    print(df)
