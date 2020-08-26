c = None


def test_main():


    def test():
        # nonlocal c
        global c
        if c:
            print('c:', c)
        else:
            c = 3
            print('init c :', c)

    test()
    test()


if __name__ == '__main__':
    # test()
    # test()
    test_main()
    test_main()
