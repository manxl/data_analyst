from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, CHAR, DATE
from flask_sqlalchemy import SQLAlchemy
from datetime import date

db = SQLAlchemy()


class BaseModel(object):

    def save(self):
        db.session.add(self)
        # db.session.commit()

    def delete(self):
        db.session.delete(self)
        # db.session.commit()


class User(BaseModel, db.Model):
    __tablename__ = 'user'
    id = Column(Integer, primary_key=True)
    username = Column(String(80), nullable=False)
    age = Column(Integer)
    email = Column(String(120), unique=True)

    def __init__(self, username, age, email):
        self.username = username
        self.age = age
        self.email = email

    def __repr__(self):
        return 'users:{},age:{},email:{}'.format(self.username, self.age, self.email)


class His(BaseModel, db.Model):
    __tablename__ = 'his'
    id = Column(Integer, primary_key=True, autoincrement=True)
    table_name = Column(String(40), nullable=False)
    biz_code = Column(String(40), nullable=True)
    end_date = Column(DATE)
    y = Column(Integer)
    m = Column(Integer)
    d = Column(Integer)

    def __init__(self, table_name, biz_code, end_date=None):
        self.table_name = table_name
        self.biz_code = biz_code
        self.end_date = end_date

    def init(self):
        self.end_date = date.today()
        self.y = self.end_date.year
        self.m = self.end_date.month
        self.d = self.end_date.day
        return self

    def __repr__(self):
        return 'end_date:{}'.format(self.end_date)
