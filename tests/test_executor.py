from kong.executor import SerialExecutor


def test_SerialExecutor():
    ex = SerialExecutor()

    results = []

    func = lambda n: results.append(n)

    f = ex.submit(func, 1)
    assert results == [1]
    assert f.exception() is None

    f = ex.submit(func, 42)
    assert results == [1, 42]
    assert f.exception() is None

    def error():
        raise ValueError()

    f = ex.submit(error)
    assert isinstance(f.exception(), ValueError)

    ret = lambda x: x * x

    f = ex.submit(ret, 4)
    assert f.result() == 16
