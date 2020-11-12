import tushare as ts
import numpy as np
import pandas as pd
from dao.db_pool import get_engine


def df_add_y_m(df, column_name):
    loc = np.where(df.columns.values.T == column_name)[0][0]
    loc = int(loc) + 1
    y = df[column_name].apply(lambda s: int(s[:4]))
    t = df[column_name].apply(lambda s: int(s[4:6]))

    df.insert(loc, 'm', t)
    df.insert(loc, 'y', y)


def df_add_y(df, column_name):
    loc = np.where(df.columns.values.T == column_name)[0][0]
    loc = int(loc) + 1
    y = df[column_name].apply(lambda s: int(s[:4]))
    df.insert(loc, 'y', y)


def drop_more_nan_row(df, column_name):
    grouped = df.groupby(column_name)
    mask = []
    for k in grouped.groups:
        row_nums = grouped.groups[k]
        min = 999999
        t = 0
        for row_num in row_nums:
            n = df.loc[row_num].isnull().sum()
            if n < min:
                min = n
                t = row_num
        mask.append(t)
    return df.loc[mask]


def drop_row_by_cal(d1, d2, col_name):
    """
    过滤dataFrame1 内 dataFrame2 重复值重复的列
    """
    l = d2[col_name].values.tolist()
    flag = d1[col_name].apply(lambda x: x not in l)
    return d1[flag]


def need_pull_check(code, table_name, force=None, condition_column='ts_code'):
    if force is None:
        sql = "select count(*) from {} where {} = '{}';".format(table_name, condition_column, code)
        # con = get_engine().connect()
        try:
            # size = get_engine().execute(sql).fetchone()[0]
            df = pd.read_sql_query(sql, get_engine())
            size = df.iloc[0, 0]
        except Exception as e:
            if 'f405' == e.code:
                return True
            else:
                print(e)
                exit(4)
        # finally:
        #     con.close()

        return False if size > 0 else True
    else:
        if 'delete' == force:
            sql = "delete from {} where {} = '{}';".format(table_name, condition_column, code)
        elif 'drop' == force:
            sql = "drop table {};".format(table_name)
        else:
            print('need_pull_check {} force flag error:{}'.format(table_name, force))
            exit(4)

        try:
            r = pd.read_sql_query(sql, get_engine()).iloc[0, 0]
            print('force clean {} rows 4 {}'.format(r.rowcount, code))
        except Exception as e:
            if 'f405' == e.code or 'e3q8' == e.code:
                return True
            else:
                print(e)
                exit(4)

        return True
