import random

stat_h = 0
stat_l = 0


def get_rate_a():
    t = random.randint(50, 200)
    t = t / 100
    return t


def get_rate_b():
    t = random.randint(50, 180)
    t = t / 100
    return t


def t():
    cap = 1000
    cap_start = cap

    for i in range(1, 10):
        cap_start = cap
        r1 = get_rate_a()
        r2 = get_rate_b()
        cap_a = cap / 2
        cap_b = cap / 2
        cap_a = cap_a * r1
        cap_b = cap_b * r2
        cap = cap_a + cap_b
        # print(f'cap_start:{cap_start}\tr1:{r1}\tcap_a:{cap_a}\tr2:{r2}\tcap_b:{cap_b}\tcap_end:{cap}')
    global stat_h, stat_l
    if cap > cap_start:
        stat_h = stat_h + 1
    else:
        stat_l = stat_l + 1


for i in range(1, 1000000):
    t()

print(stat_h, stat_l)
