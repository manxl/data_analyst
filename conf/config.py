import logging
LOG_FROMAT = "%(asctime)s\t%(threadName)s\t%(filename)s\t[%(lineno)d]\t%(levelname)s\t%(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT = None
logging.basicConfig(level=logging.DEBUG, format=LOG_FROMAT, datefmt=DATE_FORMAT, filemode='w')





# DB config
SCHEMA = 'analyst'
HOST = '127.0.0.1'
PASSWORD = '111'
USER = 'manxl'
PORT = 3306

# thread config
MULTIPLE = 10

TEST_TS_CODE_1 = '600036.SH'
TEST_TS_CODE_2 = '002230.SZ'
TEST_TS_CODE_3 = '601318.SH'
TEST_TS_CODE_4 = '300437.SZ'

TEST_INDEX_CODE_1 = '399300.SZ'

YEAR_START = 1990
YEAR_END = 2020









