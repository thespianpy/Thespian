import time

def timeit(func):
    def _timer(*args, **kw):
        start = time.time()
        ret = func(*args, **kw)
        end = time.time()
        print('%s(%s, %s) -- %2.2f sec' % (func.__name__, args, kw, end-start))
        return ret
    return _timer
