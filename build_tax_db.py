from redis_pool import redis_pool

pipeline = redis_pool.pipeline()
from utils import generate_tax_name_redis_key

with open('DB/tax_names.txt', 'rb') as f:
    for idx, line in enumerate(f.xreadlines()):
        value, key = line.strip().split('\t')

        pipeline.set(generate_tax_name_redis_key(key), value)
        pipeline.set("tax.values.{}".format(value), key)

        if idx % 100 == 0:
            print "Done: {}".format(idx)

pipeline.execute()
print "DONE!"
