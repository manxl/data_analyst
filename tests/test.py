class C:
    def __init__(self, code, kk):
        print('init')
        self._key = code
        self.kk = None

    @property
    def ts_code(self):
        return self._key

    @ts_code.setter
    def ts_code(self, value):
        self._key = value

    def process(self):
        print(self.ts_code)


def test_class():
    a = C('adsaa', 'kk_123')
    print(a.ts_code)
    print('=' * 32)
    m = {'ts_code': 333, 'kk': 'kk_123'}
    s = 'ts_code:{ts_code}\tkk:'.format(**m)

    print('=' * 32)
    print(a.ts_code, a.kk)


for i in range(4):
    print(i)




