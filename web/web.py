import sys
import dao.db_pool as pool
import pandas as pd
import matplotlib.pyplot as mp
# sys.path.append("D:\storage\workspaces\idea-2020\data_analyst")
import analyse.calc as calc
from dao.tushare_dao import *
from flask import Flask, request, render_template
import logging

LOG_FROMAT = "%(asctime)s\t%(filename)s\t[line:%(lineno)d]\t%(levelname)s\t%(message)s"
DATE_FORMAT = "%a,%d %b %Y-%m-%d %H:%M:%S %p"
logging.basicConfig(level=logging.DEBUG, format=LOG_FROMAT, datefmt=DATE_FORMAT, filemode='w')

app = Flask(__name__)

# https://www.w3cschool.cn/flask/flask_url_building.html
@app.route('/hello/<int:age>/<float:price>/')
def hello(age, price):
    return 'age is {} \t price is {}'.format(age, price)


@app.route('/error/')
def error():
    return 'aaa'


@app.route('/')
def root():
    return render_template('main.html')


@app.route('/meta/reload_income')
def reload_income():
    force = request.args['force']
    # income_dao.process()
    return render_template('main.html')


@app.route('/ml')
def test_graph122():
    df = pd.read_sql_query("select * from stock_balancesheet where ts_code = '000001.SZ' and m = 12;", pool.get_engine())

    mp.plot(df['end_date'], df['total_cur_assets'], label='123')
    mp.savefig('D:/storage/workspaces/idea-2020/data_analyst/web/static/images/new_plot1.png')
    return render_template('abc.html', name=df, url='static/images/new_plot1.png')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)
