import sys, os
import dao.db_pool as pool
import pandas as pd
import matplotlib.pyplot as mp
# sys.path.append("D:\storage\workspaces\idea-2020\data_analyst")
import analyse.calc as calc
from dao.tushare_dao import *
from werkzeug.utils import secure_filename
from flask import Flask, request, render_template, redirect, url_for, make_response, session, abort, flash
import logging

LOG_FROMAT = "%(asctime)s\t%(filename)s\t[line:%(lineno)d]\t%(levelname)s\t%(message)s"
DATE_FORMAT = "%a,%d %b %Y-%m-%d %H:%M:%S %p"
logging.basicConfig(level=logging.DEBUG, format=LOG_FROMAT, datefmt=DATE_FORMAT, filemode='w')

app = Flask(__name__)
app.secret_key = 'fkdjsafjdkfdlkjfadskjfadskljdsfklj'
app.config['UPLOAD_FOLDER'] = 'temp/'


# app.config['MAX_CONTENT_PATH'] = os.getcwd()+'/temp';

@app.route('/demo')
def demo():
    return render_template('demo.html')


@app.route('/hello_url/<name1>')
def hello_method(name1):
    return 'hello ' + name1


# http://localhost/params/1231/2.1/dddd/?force=kknd
@app.route('/params/<int:age>/<float:price>/<name>/')
def params(age, price, name):
    """
        flask route & param demo
    """
    if request.values.get('force'):
        force = request.args['force']
    else:
        force = 'NONE'
    return 'age is [{}] <br> price is [{}] <br> name is [{}] <br> force is [{}]'.format(age, price, name, force)


@app.route('/redirect/url')
def redirect_url():
    return redirect('/hello_url/redirect_url')


@app.route('/redirect/method')
def redirect_method():
    return redirect(url_for('hello_method', name1='redirect_method'))


@app.route('/methods/post', methods=['POST', 'GET'])
def methods_post():
    logging.debug(request.method)
    if request.method == 'POST':
        name = request.form['name']
    elif request.method == 'GET':
        name = request.args.get('name')
    else:
        request.values.get('name')
    return 'method:{}<br>name:{}'.format(request.method, name)


@app.route('/template')
def methods_templte():
    my_int = 18
    my_str = 'curry'
    my_list = [1, 5, 4, 3, 2]
    my_dict = {
        'name': 'durant',
        'age': 28
    }

    return render_template('4template.html',
                           my_int=my_int,
                           my_str=my_str,
                           my_list=my_list,
                           my_dict=my_dict)


@app.route('/cookies/set')
def cookies_set():
    resp = make_response('success')
    resp.set_cookie('my_cookies_key', 'my_cookies_val', max_age=30)
    return resp


@app.route('/cookies/get')
def cookies_get():
    cookie_1 = request.cookies.get('my_cookies_key')
    if cookie_1:
        return cookie_1
    else:
        return 'NONE'


@app.route('/cookies/delete')
def cookies_delete():
    resp = make_response('del success')
    resp.delete_cookie('my_cookies_key')
    return resp


@app.route('/session/set')
def session_set():
    key = 'username'
    setter = request.values.get(key)
    if setter:
        session[key] = setter
    else:
        session[key] = 'admin'
    return session[key]


@app.route('/session/get')
def session_get():
    if 'username' not in session:
        return 'NONE'
    else:
        return session['username']


@app.route('/error/403')
def error_in():
    abort(403)


@app.route('/session/delete')
def session_delete():
    return session.pop('username')


@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        if request.form['username'] != 'admin' or \
                request.form['password'] != 'admin':
            error = 'Invalid username or password. Please try again!'
    else:
        flash('You were successfully logged in')
        flash('========================')
        return redirect(url_for('index'))
    return render_template('login.html', error=error)


@app.route('/idx')
def index():
    return render_template('index.html')


@app.route('/test')
def test():
    print(os.getcwd())
    return 'ok'


@app.route('/uploader', methods=['POST'])
def uploader():
    f = request.files['file']
    f.save(os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(f.filename)))
    return 'upload suceess'


#####################  u age #######################

@app.route('/meta/reload_income')
def reload_income():
    force = request.args['force']
    # income_dao.process()
    return render_template('main.html')


@app.route('/ml')
def test_graph122():
    df = pd.read_sql_query("select * from stock_balancesheet where ts_code = '000001.SZ' and m = 12;",
                           pool.get_engine())

    mp.plot(df['end_date'], df['total_cur_assets'], label='123')
    mp.savefig('D:/storage/workspaces/idea-2020/data_analyst/web/static/images/new_plot1.png')
    return render_template('abc.html', name=df, url='static/images/new_plot1.png')


####################################################
@app.route('/')
def root():
    return render_template('main.html')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)
