import sys

sys.path.append("D:\storage\workspaces\idea-2020\data_analyst")

import analyse.calc as calc
from flask import Flask, request

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello World'


@app.route('/graph')
def test_graph():
    print(1)
    calc.show()
    return 'Hello World show'


@app.route('/abb')
def test_graph1():
    print(1)
    print(2)
    a = request.args.get('p')
    b = request.args.get('pa')
    c = request.args.get('papa')
    print(a)
    print(b)
    print(c)

    return 'abcd'


if __name__ == '__main__':
    app.run(host='10.4.136.156', port=80, debug=True)
