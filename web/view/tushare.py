from flask import Blueprint

main = Blueprint('ts', __name__)

from web.app import fdb
from web.model.pojo import User
from flask import request, render_template
import pandas as pd
from dao.db_pool import get_engine
import matplotlib.pyplot as mp


@main.route('/meta/reload_income')
def reload_income():
    force = request.args['force']
    # income_dao.process()
    return render_template('demo/main.html')


@main.route('/ml')
def test_graph122():
    df = pd.read_sql_query("select * from stock_balancesheet where ts_code = '000001.SZ' and m = 12;",
                           get_engine())

    mp.plot(df['end_date'], df['total_cur_assets'], label='123')
    mp.savefig('D:/storage/workspaces/idea-2020/data_analyst/web/static/images/new_plot1.png')
    return render_template('demo/abc.html', name=df, url='static/images/new_plot1.png')


####################################################
@main.route('/db/insert')
def db_insert():
    users = User('罗汉', 40, '2aluohan12@sina.com')
    fdb.session.add(users)
    fdb.session.commit()
    return 'insert ok'


@main.route('/db/select')
def db_select():
    users = User('罗汉', 38, 'aluohan1@sina.com')
    a = fdb.session.query(User)
    print(a)
    r = a.all()
    print(r)
    s = ""
    for u in r:
        s += 'a:{},b:{},c:{}<br>'.format(u.id, u.age,u.username)

    # o = User.query.filter_by(id=3)
    o = User.query.filter_by(username='罗汉')
    s +='<br><br>' + o.one().__repr__()

    return s


# create
# fdb.create_all()


####################################################
@main.route('/')
def root():
    return render_template('demo/main.html')
