from flask import Blueprint

demo = Blueprint('demo', __name__, template_folder='../templates/demo')

from flask import Flask, request, render_template, redirect, url_for, make_response, session, abort, flash
import logging
from conf.config import FLASK_UPLOAD_FOLDER
import os
from werkzeug.utils import secure_filename


@demo.route('/list')
def demo_index():
    from web.model.pojo import User
    us = User.query.all()
    return render_template('list.html', user_list=us)


@demo.route('/hello_url/<name>')
def hello_method(name):
    return 'hello ' + name


# http://localhost/params/1231/2.1/dddd/?force=kknd
@demo.route('/params/<int:age>/<float:price>/<name>/')
def params(age, price, name):
    """
        flask route & param demo
    """
    if request.values.get('force'):
        force = request.args['force']
    else:
        force = 'NONE'
    return 'age is [{}] <br> price is [{}] <br> name is [{}] <br> force is [{}]'.format(age, price, name, force)


@demo.route('/redirect/url')
def redirect_url():
    return redirect('/demo/hello_url/redirect_url')


@demo.route('/redirect/method')
def redirect_method():
    return redirect(url_for('demo.hello_method', name='redirect_method'))


@demo.route('/methods/post', methods=['POST', 'GET'])
def methods_post():
    logging.debug(request.method)
    if request.method == 'POST':
        name = request.form['name']
    elif request.method == 'GET':
        name = request.args.get('name')
    else:
        request.values.get('name')
    return 'method:{}<br>name:{}'.format(request.method, name)


@demo.route('/template')
def methods_templte():
    template_name = '4template.html'
    return comm_template(template_name)


@demo.route('/template/child')
def templte_sub():
    template_name = 'child.html'
    return comm_template(template_name)


@demo.route('/template/super')
def templte_super():
    template_name = 'super.html'
    return comm_template(template_name)


def comm_template(template_name):
    my_int = 18
    my_str = 'curry'
    my_list = [1, 5, 4, 3, 2]
    my_dict = {
        'name': 'durant',
        'age': 28
    }
    return render_template(template_name,
                           my_int=my_int,
                           my_str=my_str,
                           my_list=my_list,
                           my_dict=my_dict)


@demo.route('/cookies/set')
def cookies_set():
    resp = make_response('success')
    resp.set_cookie('my_cookies_key', 'my_cookies_val', max_age=30)
    return resp


@demo.route('/cookies/get')
def cookies_get():
    cookie_1 = request.cookies.get('my_cookies_key')
    if cookie_1:
        return cookie_1
    else:
        return 'NONE'


@demo.route('/cookies/delete')
def cookies_delete():
    resp = make_response('del success')
    resp.delete_cookie('my_cookies_key')
    return resp


@demo.route('/session/set')
def session_set():
    key = 'username'
    setter = request.values.get(key)
    if setter:
        session[key] = setter
    else:
        session[key] = 'admin'
    return session[key]


@demo.route('/session/get')
def session_get():
    if 'username' not in session:
        return 'NONE'
    else:
        return session['username']


@demo.route('/error/403')
def error_in():
    abort(403)


@demo.route('/session/delete')
def session_delete():
    return session.pop('username')


@demo.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        if request.form['username'] != 'admin' or \
                request.form['password'] != 'admin':
            error = 'Invalid username or password. Please try again!'
    else:
        flash('You were successfully logged in')
        flash('========================')
        return redirect(url_for('demo.scripts'))
    return render_template('login.html', error=error)


@demo.route('/scripts')
def scripts():
    return render_template('scripts.html')


@demo.route('/test')
def test():
    print(os.getcwd())
    return 'ok1'


@demo.route('/uploader', methods=['POST'])
def uploader():
    f = request.files['file']
    f.save(os.path.join(FLASK_UPLOAD_FOLDER, secure_filename(f.filename)))
    return 'upload suceess'


@demo.route('/db/insert')
def db_insert():
    from web.model.pojo import User
    u = User('罗汉1', 40, '1aluohan12@sina.com')
    u.save()

    return 'insert ok'


@demo.route('/db/select')
def db_select():
    from web.model.pojo import User

    r = User.query.all()

    s = ""
    for u in r:
        s += 'a:{},b:{},c:{}<br>'.format(u.id, u.age, u.username)

    # o = User.query.filter_by(id=3)
    o = User.query.filter_by(id=1).one()
    s += '<br><br>' + o.__repr__()
    o.username = o.username + "123"
    # o.add_update()
    return s


@demo.route('/db/rollback')
def db_rollback():
    from web.model.pojo import User
    u = User('罗汉5', 40, '5aluohan12@sina.com')
    u.save()
    # print(1/0)
    return 'insert ok'


@demo.route('/db/delete/<uid>')
def db_delete(uid):
    from web.model.pojo import User
    us = User.query.filter_by(id=uid)
    u = us.one()
    u.delete()
    return redirect(url_for('demo.demo_index'))
