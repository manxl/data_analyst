# import sys
# sys.path.append("D:\storage\workspaces\idea-2020\data_analyst")
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import logging
from conf.config import FLASK_SQLALCHEMY_DATABASE_URI,FLASK_UPLOAD_FOLDER,FLASK_SECRET_KEY
from web.view.tushare import main
from web.view.demo import demo

LOG_FORMAT = "%(asctime)s\t%(filename)s\t[line:%(lineno)d]\t%(levelname)s\t%(message)s"
DATE_FORMAT = "%a,%d %b %Y-%m-%d %H:%M:%S"
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT, datefmt=DATE_FORMAT, filemode='w')

app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY
app.config['UPLOAD_FOLDER'] = FLASK_UPLOAD_FOLDER
app.config['SQLALCHEMY_DATABASE_URI'] = FLASK_SQLALCHEMY_DATABASE_URI

fdb = SQLAlchemy(app)


app.register_blueprint(main)


app.register_blueprint(demo, url_prefix='/demo')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)
