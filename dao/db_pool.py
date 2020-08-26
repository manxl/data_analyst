from DBUtils.PooledDB import PooledDB, SharedDBConnection
from DBUtils.PersistentDB import PersistentDB
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import yaml, os, re, pymysql, time, xlwt, sys, hashlib, sqlalchemy
from time import sleep, time
from sqlalchemy import create_engine
import conf.config as config

engine = None
def get_engine():
    global engine
    if engine:
        return engine

    engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8".format(
        config.USER, config.PASSWORD, config.HOST, config.PORT, config.SCHEMA))
    print('='*64,'engine inited')
    return engine


class MySQL:
    __pool = None

    @staticmethod
    def get_connection():
        # 构造函数，创建数据库连接、游标
        if __class__.__pool is None:
            __class__.__pool = PooledDB(
                pymysql,
                10,
                host='127.0.0.1',
                user='manxl',
                port=3306,
                passwd='111',
                # db='analyst',
                # use_unicode=True，
                charset='utf8')
        # return __class__.__pool.connection()
        return __class__.__pool.connection()

    def get_all(self, sql, param=None):
        """
        @summary: 执行查询，并取出所有结果集
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list(字典对象)/boolean 查询到的结果集
        """
        con = __class__.get_connection()
        cur = con.cursor(pymysql.cursors.DictCursor)
        try:
            if param is None:
                count = cur.execute(sql)
            else:
                count = cur.execute(sql, param)
            if count > 0:
                result = cur.fetchall()
            else:
                result = False
        except Exception as e:
            # con.rollback()  # 事务回滚
            print('SQL执行有误,原因:', e)
            result = None
        finally:
            cur.close()
            con.close()

        return result

    def get_one(self, sql, param=None):
        con = self.__pool.connection()
        cur = con.cursor(pymysql.cursors.DictCursor)
        try:
            if param is None:
                count = cur.execute(sql)
            else:
                count = cur.execute(sql, param)
            if count > 0:
                result = cur.fetchone()
            else:
                result = False
        except Exception as e:
            # con.rollback()  # 事务回滚
            print('SQL执行有误,原因:', e)
            result = None
        finally:
            cur.close()
            con.close()
        return result

    def getMany(self, sql, num, param=None):
        """
        @summary: 执行查询，并取出num条结果
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param num:取得的结果条数
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list/boolean 查询到的结果集
        """
        if param is None:
            count = cur.execute(sql)
        else:
            count = cur.execute(sql, param)
        if count > 0:
            result = cur.fetchmany(num)
        else:
            result = False
        return result

    def insertOne(self, sql, value):
        """
        @summary: 向数据表插入一条记录
        @param sql:要插入的ＳＱＬ格式
        @param value:要插入的记录数据tuple/list
        @return: insertId 受影响的行数
        """
        cur.execute(sql, value)
        return self.__getInsertId()

    def insertMany(self, sql, values):
        """
        @summary: 向数据表插入多条记录
        @param sql:要插入的ＳＱＬ格式
        @param values:要插入的记录数据tuple(tuple)/list[list]
        @return: count 受影响的行数
        """
        count = cur.executemany(sql, values)
        return count

    def __getInsertId(self):
        """
        获取当前连接最后一次插入操作生成的id,如果没有则为０
        """
        self.__cur.execute("SELECT @@IDENTITY AS id")
        result = cur.fetchall()
        return result[0]['id']

    def __query(self, sql, param=None):
        if param is None:
            count = cur.execute(sql)
        else:
            count = cur.execute(sql, param)
        return count

    def update(self, sql, param=None):
        """
        @summary: 更新数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要更新的  值 tuple/list
        @return: count 受影响的行数
        """
        return self.__query(sql, param)

    def delete(self, sql, param=None):
        """
        @summary: 删除数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要删除的条件 值 tuple/list
        @return: count 受影响的行数
        """
        return self.__query(sql, param)

    def begin(self):
        """
        @summary: 开启事务
        """
        self.__pool.autocommit(0)

    def end(self, option='commit'):
        """
        @summary: 结束事务
        """
        if option == 'commit':
            self.__pool.commit()
        else:
            self.__pool.rollback()

    def dispose(self, flag):
        if flag == 1:
            self.end('commit')
        elif flag == 0:
            self.end('rollback')
        self.__pool.close()


def func(pool):
    print('start')
    sql = 'select count(*) from  zun_flush.act_activity_notice	where LENGTH(creater) = %s;'
    a = pool.get_all(sql, param=19)
    a = a[:]
    for i in a:
        print(a)
    sleep(2)
    pool.dispose(-1)
    print('1111')
    return 'ok'


if __name__ == '__main__':
    start = time()
    pool = MySQL()
    threadPool = ThreadPoolExecutor(max_workers=5, thread_name_prefix="test_")
    fs = []
    for i in range(1, 10):
        f = threadPool.submit(func, pool)
        fs.append(f)

    for f in fs:
        print(f.result())
    print('main waiting')
    threadPool.shutdown(wait=True)
    print('main over:{}'.format(time() - start))
    sleep(10)
    print('main out')

    # for i in a:
    #     print(i)
    # pool.begin()
    #         sql = 'update test.test_user set name= %s where id = %s'
    #         a = pool.update(sql, ('aa1', 1))
    #         b = pool.update(sql, ('bb1', 2))
    #         print(a, '-' * 32, b)
    #         pool.end('commit')
    # pool.dispose(isEnd=1)
