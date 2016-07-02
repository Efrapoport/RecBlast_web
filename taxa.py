from redis_pool import redis_pool


def generate_tax_name_redis_key(name):
    return "tax.names.{}".format(name.lower().replace(' ', '-'))


def get_name_by_value(value):
    result = redis_pool.get('tax.values.{}'.format(value))
    if result:
        return result

    raise KeyError(value)


def get_value_by_name(name):
    result = redis_pool.get(generate_tax_name_redis_key(name))

    if result:
        return result

    raise KeyError(name)
