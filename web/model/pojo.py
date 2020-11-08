from web.web_base import fdb


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
            return '<users:%r' % self.username
