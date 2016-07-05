# building the tax db once and adding it to the redis service

from redis_pool import redis_pool
from taxa import generate_tax_name_redis_key
from RecBlastUtils import strip, split

# pipeline = redis_pool.pipeline()  # changed later because of low memory on web server

# doing it gradually and slowly to avoid memory overload
with open('DB/tax_names.txt', 'rb') as f:
    for idx, line in enumerate(f.xreadlines()):
        value, key = split(strip(line), '\t')

        redis_pool.set(generate_tax_name_redis_key(key), value)
        redis_pool.set("tax.values.{}".format(value), key)

        if idx % 100 == 0:
            print "Done: {}".format(idx)

# redis_pool.execute()
print "DONE!"
