import pandas as pd
import re
import os,sys

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
    tpl_float = "'{}': Float(precision=53),"
    column_list = ''
    define = '{'
    for i, row in df.iterrows():
        # if i % 3 == 0 and i != 0:
        #     define += '\n'
        name = row['name']
        column_list += (name + ',')
        t = row['type']
        if 'str' == t:
            if '_date' in name:
                define += tpl_date.format(name)
                if name == 'end_date':
                    define += "'y': INT(), 'm': INT(),"
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

    print('column_list:', column_list.strip(','))


def match_names(file_path, dict_sheet, report_sheet):
    df_d = pd.read_excel(file_path, dict_sheet)
    df_r = pd.read_excel(file_path, report_sheet)

    d = {}
    for i, row in df_d.iterrows():
        d[row['desc']] = row['name']

    for i, row in df_r.iterrows():
        name = row['name']
        if name is not None:
            name = name.strip('：')
            name = name.replace("其中：", "")
        if name in d:
            print('{} - > {}'.format(name, d[name]))
        else:
            print('{} ==='.format(name))


if __name__ == '__main__':
    p = os.path.dirname
    file_path = os.path.join(p(p(__file__)),'data/define/define.xlsx')
    sheet_name = 'divident'
    dtype_4_fina_generator(file_path, sheet_name)
