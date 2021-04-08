import sys, re, os

data_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'balance')

with open(data_file, mode='r', encoding='utf8') as f:
    all = f.read()
    no_more_line_end = re.sub(r'\n{2,4}', '\n', all)
    no_more_page_num = re.sub(r'\n\s+\d+\n', '\n', no_more_line_end)
    no_more_page_brow = re.sub(r'\n\s{10,30}[\u4e00-\u9fa5\d\s]+\n', '\n', no_more_page_num)
    combine_1 = re.sub(r'\n\s+(（[\u4e00-\u9fa5]{2,3})\n(\s+[\u4e00-\u9fa5]+)\s+([\d,\.]+)\n\s+([\u4e00-\u9fa5]）)\n',
                       r'\n\2\t\1\4\t\3-------\n', no_more_page_brow)
    combine_2 = re.sub(
        r'\n\s+(（[\u4e00-\u9fa5]{2,3})\n(\s+[\u4e00-\u9fa5]+[（）\u4e00-\u9fa5]*)\s+([\d,\.]+)\s+([\d,\.]+)\n\s+([\u4e00-\u9fa5]）)\n',
        r'\n\2\t\1\5\t\3\t\4\n', combine_1)
    combine_2 = re.sub(
        r'\n\s+(（[\u4e00-\u9fa5]{2,3})\n(\s+[\u4e00-\u9fa5]+[（）\u4e00-\u9fa5]*)\s+([\d,\.]+)\s+([\d,\.]+)\n\s+([\u4e00-\u9fa5]）)\n',
        r'\n\2\t\1\5\t\3\t\4\n', combine_2)

    tab_without_1 = re.sub(r'\n(\s+[\u4e00-\u9fa5]+[（）\u4e00-\u9fa5]*)\s+([\d,\.]+)\n',
                           r'\n\1\t\2\n----', combine_2)

    tab_without_2 = re.sub(r'\n(\s+[\u4e00-\u9fa5]+[（）\u4e00-\u9fa5]*)\x20+([\d,\.]+)\x20+([\d,\.]+)\n',
                           r'\n\1\t\t\2\t\3\n', tab_without_1)
    tab_without_2 = re.sub(r'\n(\s+[\u4e00-\u9fa5]+[（）\u4e00-\u9fa5]*)\x20+([\d,\.]+)\x20+([\d,\.]+)\n',
                           r'\n\1\t\t\2\t\3\n', tab_without_2)

    tab_with_1 = re.sub(r'\n(\s+[\u4e00-\u9fa5]+[（）\u4e00-\u9fa5]*)\x20+(（[\u4e00-\u9fa5]{1,3}）)\x20+([\d,\.]+)\n',
                        r'\n\1\t\2\t\3---------\n', tab_without_2)

    tab_with_2 = re.sub(
        r'\n(\s+[\u4e00-\u9fa5]+[（）\u4e00-\u9fa5]*)\x20+(（[\u4e00-\u9fa5]{1,3}[）\)])\x20+([\d,\.]+)\s+([\d,\.]+)\n',
        r'\n\1\t\2\t\3\t\4\n', tab_with_1)
    tab_with_2 = re.sub(
        r'\n(\s+[\u4e00-\u9fa5]+[（）\u4e00-\u9fa5]*)\x20+(（[\u4e00-\u9fa5]{1,3}[）\)])\x20+([\d,\.]+)\s+([\d,\.]+)\n',
        r'\n\1\t\2\t\3\t\4\n', tab_with_2)

    too_long = re.sub(
        r'\n(\s+[\u4e00-\u9fa5（]+)\n\s?([\u4e00-\u9fa5）：]+)\n',
        r'\n\1\2\n', tab_with_2)

    print(too_long)
