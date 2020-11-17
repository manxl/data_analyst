# import sys
# sys.path.append("D:\storage\workspaces\idea-2020\data_analyst")


def __init():
    from flask import Flask
    from conf.config import FLASK_SQLALCHEMY_DATABASE_URI, FLASK_UPLOAD_FOLDER, FLASK_SECRET_KEY

    app = Flask(__name__)
    # 初始化session
    app.secret_key = FLASK_SECRET_KEY
    # 文件上传目录
    app.config['UPLOAD_FOLDER'] = FLASK_UPLOAD_FOLDER
    # 数据库初始化
    app.config['SQLALCHEMY_DATABASE_URI'] = FLASK_SQLALCHEMY_DATABASE_URI
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
    # app.config['SQLALCHEMY_COMMIT_ON_TEARDOWN'] = True

    from web.model.pojo import db
    db.init_app(app)

    db.create_all(app=app)

    # 注册蓝图
    from web.view.ts_view import main
    app.register_blueprint(main)

    from web.view.demo_view import demo
    app.register_blueprint(demo, url_prefix='/demo')

    app.add_template_global(type,"type")
    app.add_template_global(len,"len")
    app.add_template_global(list,"list")

    return app


if __name__ == '__main__':
    app1 = __init()
    app1.run(host='0.0.0.0', port=80, debug=True)
