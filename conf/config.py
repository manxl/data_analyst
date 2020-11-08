import logging

LOG_FROMAT = "%(asctime)s\t%(threadName)s\t%(filename)s\t[%(lineno)d]\t%(levelname)s\t%(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT = None
logging.basicConfig(level=logging.DEBUG, format=LOG_FROMAT, datefmt=DATE_FORMAT, filemode='w')

FLASK_SECRET_KEY = 'fkdjsafjdkfdlkjfadskjfadskljdsfklj'
FLASK_UPLOAD_FOLDER = 'static/temp/'
FLASK_SQLALCHEMY_DATABASE_URI = 'mysql://manxl:111@localhost:3306/analyst'

# DB config
SCHEMA = 'analyst'
HOST = '127.0.0.1'
PASSWORD = '111'
USER = 'manxl'
PORT = 3306

####### stock list
# thread config
MULTIPLE = 10
#
TEST_TS_CODE_1_ZSYN = '600036.SH'
#
TEST_TS_CODE_2_KDXF = '002230.SZ'
# zgpa
TEST_TS_CODE_3_ZGPA = '601318.SH'
# qsy
TEST_TS_CODE_4_QSY = '300437.SZ'
# yhgf
TEST_TS_CODE_5_YHGF = '002304.SZ'

######## index
# 300
TEST_INDEX_CODE_1 = '399300.SZ'

YEAR_START = 1990
YEAR_END = 2020
