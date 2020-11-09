from conf.config import Y, M, D, OPERATION_TRUNCATE, OPERATION_APPEND, ERROR_NOT_INITED
import pandas as pd
from dao.db_pool import get_engine
import abc, logging


class History:
    def __init__(self, table):
        self.table = table
        self.end_date = None

    def __int__(self, table, end_date):
        self.table = table
        self.end_date = end_date

    def init(self):
        if self.t == 'y':
            self.y = self.end_date.apply(lambda s: int(s[:4]))
        elif self.t == 'm':
            self.y = self.end_date.apply(lambda s: int(s[:4]))
            self.m = self.end_date.apply(lambda s: int(s[4:6]))
        elif self.t == 'd':
            self.y = self.end_date.apply(lambda s: int(s[:4]))
            self.m = self.end_date.apply(lambda s: int(s[4:6]))
            self.d = self.end_date.apply(lambda s: int(s[6:]))
        return self


class BaseController:
    def __init__(self, interface):
        logging.debug('Init Controller {}'.format(self.__class__))
        self.interface = interface

    def _get_table_name(self):
        return self.interface

    def process(self):
        history = self.get_history()
        if ERROR_NOT_INITED == history:
            self.init_history()
            self.init()
        elif history is None:
            self.init()
        elif type(history) is History:
            print('HISTORY')
        else:
            exit(4)

    def get_history(self):
        sql = "select * from history where table_name = '{}';".format(self._get_table_name())
        # con = get_engine().connect()
        try:
            history = get_engine().execute(sql).fetchone()

            return history
        except Exception as e:
            if ERROR_NOT_INITED == e.code:
                return e.code
            else:
                print(e)
                exit(4)

    def init_history(self):
        logging.debug('Init history table.')
        sql = """CREATE TABLE `history` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `table_name` varchar(80) NOT NULL,
  `end_date` date DEFAULT NULL,
  `y` int(11) DEFAULT NULL,
  `m` int(11) DEFAULT NULL,
  `d` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8"""
        r = get_engine().execute(sql)
        print(r)

    def init(self):
        logging.debug('Init history table {}.'.format(self._get_table_name()))
        sql = """insert into history (`table_name`,end_date,y,m,d) values 
        ('{}',curdate(),YEAR(CURDATE()),MONTH(CURDATE()) ,DAY(CURDATE()))""".format(self._get_table_name())
        r = get_engine().execute(sql)
        print(r)

    @abc.abstractmethod
    def update(self):
        pass

    @abc.abstractmethod
    def destroy(self):
        pass


class StockListController(BaseController):
    def __init__(self):
        super().__init__('stock_basic')
        self.t = Y
        self.operation = OPERATION_TRUNCATE

    def update(self):
        pass

    def destroy(self):
        pass


if __name__ == '__main__':
    s = StockListController()
    s.process()
