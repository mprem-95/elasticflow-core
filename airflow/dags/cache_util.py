import redis

r = redis.StrictRedis(host='localhost', port=6379, db=0)

def get(key):
    return r.get(key)

def set(key, value):
    r.set(key,value)