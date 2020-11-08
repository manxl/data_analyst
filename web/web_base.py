import sys, os
import dao.db_pool as pool
import matplotlib.pyplot as mp
# sys.path.append("D:\storage\workspaces\idea-2020\data_analyst")
import analyse.calc as calc
from dao.tushare_dao import *
from werkzeug.utils import secure_filename
from flask import Flask, request, render_template
from flask_sqlalchemy import SQLAlchemy
import logging
import web.view.demo


LOG_FROMAT = "%(asctime)s\t%(filename)s\t[line:%(lineno)d]\t%(levelname)s\t%(message)s"
DATE_FORMAT = "%a,%d %b %Y-%m-%d %H:%M:%S %f%p"
logging.basicConfig(level=logging.DEBUG, format=LOG_FROMAT, datefmt=DATE_FORMAT, filemode='w')

app = Flask(__name__)

app.secret_key = 'fkdjsafjdkfdlkjfadskjfadskljdsfklj'
app.config['UPLOAD_FOLDER'] = 'static/temp/'
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://manxl:111@localhost:3306/analyst'

db = SQLAlchemy(app)


#####################  u age #######################


@app.route('/meta/reload_income')
def reload_income():
    force = request.args['force']
    # income_dao.process()
    return render_template('demo/main.html')


@app.route('/ml')
def test_graph122():
    df = pd.read_sql_query("select * from stock_balancesheet where ts_code = '000001.SZ' and m = 12;",
                           pool.get_engine())

    mp.plot(df['end_date'], df['total_cur_assets'], label='123')
    mp.savefig('D:/storage/workspaces/idea-2020/data_analyst/web/static/images/new_plot1.png')
    return render_template('demo/abc.html', name=df, url='static/images/new_plot1.png')


####################################################
@app.route('/db/insert')
def db_insert():
    users = User('罗汉', 38, 'aluohan1@sina.com')
    db.session.add(users)
    db.session.commit()
    return 'insert ok'


@app.route('/db/select')
def db_select():
    users = User('罗汉', 38, 'aluohan1@sina.com')
    a = db.session.query(User)
    print(a)
    r = a.all()
    print(r)
    return 'insert ok'


class User(db.Model):
    __tablename__ = 'user'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), nullable=False)
    age = db.Column(db.Integer)
    email = db.Column(db.String(120), unique=True)

    def __init__(self, username, age, email):
        self.username = username
        self.age = age
        self.email = email

        def __repr__(self):
            return '<users:%r' % self.username


# create
db.create_all()


####################################################
@app.route('/')
def root():
    return render_template('demo/main.html')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)
