import pandas as pd


def dtype_4_mysql_generator(file_path, sheet_name):
    df = pd.read_excel(file_path, sheet_name)
    tpl_str = "'{}': VARCHAR(length={}),"
    tpl_date = "'{}': DATE(),"
    tpl_float = "'{}': BIGINT(),"

    define = '{'
    for i, row in df.iterrows():
        if i % 3 == 0 and i != 0:
            define += '\n'

        name = row['name']
        t = row['type']
        if 'str' == t:
            if '_date' in name:
                define += tpl_date.format(name)
                continue
            elif 'type' in name or 'flag' in name:
                size = 1
            else:
                size = 10
            define += tpl_str.format(name, size)
        elif 'float' == t:
            define += tpl_float.format(name)
        else:
            print('type error:', t)
            exit(4)
    define = define.strip(',')
    define += '}'
    print('-' * 32)
    print(define)

def dtype_4_fina_generator(file_path, sheet_name):
    df = pd.read_excel(file_path, sheet_name)


    tpl_str = "'{}': VARCHAR(length={}),"
    tpl_date = "'{}': DATE(),"
    tpl_float = "'{}': BIGINT(),"
    tpl_float = "'{}': FLOAT(),"
    column_list =''
    define = '{'
    for i, row in df.iterrows():
        if i % 3 == 0 and i != 0:
            define += '\n'

        name = row['name']
        column_list += (name+',')
        t = row['type']
        if 'str' == t:
            if '_date' in name:
                define += tpl_date.format(name)
                continue
            elif 'type' in name or 'flag' in name:
                size = 1
            else:
                size = 10
            define += tpl_str.format(name, size)
        elif 'float' == t:
            define += tpl_float.format(name)
        else:
            print('type error:', t)
            exit(4)
    define = define.strip(',')
    define += '}'
    print('-' * 32)
    print(define)


    print('column_list:',column_list.strip(','))

if __name__ == '__main__':
    file_path = 'C:/Users/xlman.DESKTOP-V8KO3HL/Desktop/tax.xlsx'
    sheet_name = 'balancesheet'
    sheet_name = 'cashflow'
    sheet_name = 'income'
    # dtype_4_mysql_generator(file_path, sheet_name)

    sheet_name = 'indicator'
    dtype_4_fina_generator(file_path, sheet_name)
