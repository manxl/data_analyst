import pandas as pd

s1 = range(9)
s2 = list('abcdefghi')
m = {'c1': s1, 'c2': s2}
df = pd.DataFrame(m)
print(len(df))
