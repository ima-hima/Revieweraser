from time        import time

def main():
    # import redis
    # redis_db = redis.Redis(host="10.0.0.13", port=6379, db=0)
    start = time()

    for i in range(10000):
        redis_insert()

    print( time() - start )

def redis_insert():
    ''' Insert tuple (rdd) into Redis. Tuple is of form (key, [int, int, int]), where the ints are
        number of reviews, total star rating, total number of words, respectively. '''
    import redis
    redis_db = redis.Redis(host="10.0.0.13", port=6379, db=1)
    for i in range(1):
        if redis_db.exists(i):
            pass
            # redis_db.hmset(tup[0], {'num': tup[1], 'stars': tup[2], 'words': tup[3]} )
        else:
            redis_db.hmset(i, {'num': 1, 'stars': 1, 'words': 1} )

main()
