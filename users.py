from redis_pool import redis_pool

from uuid import uuid4


def has_job_for_email(email):
    return bool(redis_pool.get('users.jobs.email.{}'.format(email)))


def set_has_job_for_email(email, value=True):
    return redis_pool.set('users.jobs.email.{}'.format(email), value)


def user_id_for_email(email):
    user_id = redis_pool.get('users.ids.{}'.format(email))

    if not user_id:
        user_id = uuid4()
        redis_pool.set('users.ids.{}'.format(email), user_id)

    return user_id
