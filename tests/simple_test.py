import pandas as pd
import numpy as np
import json
import time
from kazoo.client import KazooClient
from kazoo.exceptions import *


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


def get_conn_info():
    hosts = '172.31.12.159:2181,172.31.12.163:2181,172.31.12.164:2181'
    node = '/test/lock'
    name = 'kknd'
    passwd = '123'
    return hosts, node, name, passwd


def test_zk():
    hosts, node, name, passwd = get_conn_info()

    zk = KazooClient(hosts=hosts)
    zk.start()

    node_path = node
    try:
        stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        zk.create(node_path, f'this is test! {stamp}'.encode(), makepath=True, ephemeral=True, sequence=False)
    except NodeExistsError as e:
        print(f'node_path [{node_path}] exists.')
    else:
        print('create ok!')
        node_v = zk.get(node_path)
        print('node:', node_v)
        flag = input('todo?')
        f = flag.lower()[0]
        print('-' * 32, 'get path')
        get_result = zk.get(node_path)
        print('-' * 16, get_result)

        if f == 'y':
            pass
            print('thread is stopping.')
        if f == 'd':
            print('start delete path.')
            result = zk.delete(node_path)
            print('-' * 16, 'delete result')
            print(result)
        if f == 's':
            print('start set node')
            result = zk.set(node_path, b'new set')
            input('Press any key exit.')
        if f == 'w':
            print('start watching node.')
            zk.get(node_path, watch=zk_watch)
            print('---watching')
            print('end')
            input()
        else:
            print('not!')

    zk.stop()


def zk_watch(e):
    print(e)


if __name__ == '__main__':
    test_zk()
