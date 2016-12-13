

def time_it(func, *args, **kwargs):
    import time

    t0 = time.time()
    result = func(*args, **kwargs)
    t1 = time.time()

    print("Elapsed time: %.3f" % (t1-t0))

    return result

 