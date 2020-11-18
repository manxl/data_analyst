import logging

LOG_FORMAT = "%(asctime)s\t%(filename)s\t[line:%(lineno)d]\t%(levelname)s\t%(message)s"
DATE_FORMAT = "%a,%d %b %Y-%m-%d %H:%M:%S"
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT, datefmt=DATE_FORMAT, filemode='w')

# LOG_FROMAT = "%(asctime)s\t%(threadName)s\t%(filename)s\t[%(lineno)d]\t%(levelname)s\t%(message)s"
# DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
# DATE_FORMAT = None
# logging.basicConfig(level=logging.DEBUG, format=LOG_FROMAT, datefmt=DATE_FORMAT, filemode='w')


Y = 'y'
M = 'm'
D = 'd'
OPERATION_TRUNCATE = 't'
OPERATION_APPEND = 'a'

ERROR_NOT_INITED = 'f405'

FLASK_SECRET_KEY = 'fkdjsafjdkfdlkjfadskjfadskljdsfklj'
FLASK_UPLOAD_FOLDER = 'static/temp/'
FLASK_SQLALCHEMY_DATABASE_URI = 'mysql://manxl:111@localhost:3306/analyst'

# DB config
SCHEMA = 'analyst'
HOST = '127.0.0.1'
PASSWORD = '111'
USER = 'manxl'
PORT = 3306

# process control
CTL_CYCLE_YEAR = 'y'
CTL_CYCLE_MONTH = 'm'
CTL_CYCLE_DAY = 'd'
CTL_OPERATE_TRUNCATE = 'T'
CTL_OPERATE_APPEND = 'A'
CTL_PROCESS_INIT = 'INIT'
CTL_PROCESS_UPDATE = 'UPDATE'

# thread config
MULTIPLE = 10

#
TEST_TS_CODE_ZSYH = '600036.SH'
#
TEST_TS_CODE_KDXF = '002230.SZ'
# zgpa
TEST_TS_CODE_ZGPA = '601318.SH'
# qsy
TEST_TS_CODE_QSY = '300437.SZ'
# yhgf
TEST_TS_CODE_YHGF = '002304.SZ'
# gsyh
TEST_TS_CODE_GSYH = '601398.SH'

# gsyh
TEST_TS_CODE_FZCM = '002027.SZ'


# SZ50
TEST_INDEX_CODE_SZ50 = '000016.SH'
######## index
# 300
TEST_INDEX_CODE_1 = '399300.SZ'

YEAR_START = 1990
YEAR_END = 2020

AAA = 3.97

CTL_INTERFACE_CLASS_MAPPING = {}
