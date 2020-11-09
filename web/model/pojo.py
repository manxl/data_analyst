from web.app import fdb


class User(fdb.Model):
    __tablename__ = 'user'
    id = fdb.Column(fdb.Integer, primary_key=True)
    username = fdb.Column(fdb.String(80), nullable=False)
    age = fdb.Column(fdb.Integer)
    email = fdb.Column(fdb.String(120), unique=True)

    def __init__(self, username, age, email):
        self.username = username
        self.age = age
        self.email = email

    def __repr__(self):
        return 'users:{},age:{},email:{}'.format(self.username,self.age,self.email)


class CTL(fdb.Model):
    __tablename__ = 'ctl'
    id = fdb.Column(fdb.Integer, primary_key=True,autoincrement=True)
    table = fdb.Column(fdb.String(80), nullable=False)
    t = fdb.Column(fdb.CHAR(1), nullable=False)
    end_date = fdb.Column(fdb.DATE)
    y = fdb.Column(fdb.Integer)
    m = fdb.Column(fdb.Integer)
    d = fdb.Column(fdb.Integer)

    def __init__(self, table, t, end_date):
        self.table = table
        self.t = t
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

    def __repr__(self):
        return 'users:{},age:{},email:{}'.format(self.username,self.age,self.email)
