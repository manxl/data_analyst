import pandas as pd
import dao.db_pool as pool

engine = pool.get_engine()


def get_dividend(ts_code, start, end):
    sql = "select * from stock_dividend where ts_code= '{}' and ex_date between '{}' and '{}';"
    sql = sql.format(ts_code, start, end)
    df = pd.read_sql_query(sql, engine)
    # TODO
    print('in {} {}'.format(__file__, engine))
    return df
