# building the tax db once and adding it to the redis service

from redis_pool import redis_pool
from taxa import generate_tax_name_redis_key
from RecBlastUtils import strip, split

pipeline = redis_pool.pipeline()

# doing it gradually and slowly to avoid memory overload
with open('DB/tax_names.txt', 'rb') as f:
    for idx, line in enumerate(f.xreadlines()):
        value, key = split(strip(line), '\t')

        pipeline.set(generate_tax_name_redis_key(key), value)
        pipeline.set("tax.values.{}".format(value), key)

        if idx % 100 == 0:
            print "Done: {}".format(idx)

pipeline.execute()
print "DONE!"
