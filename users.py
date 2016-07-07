from redis_pool import redis_pool
from uuid import uuid4


def has_job_for_email(email):
    """Returns True if the user already has a running job."""
    return bool(redis_pool.get('users.jobs.email.{}'.format(email)))


def set_has_job_for_email(email, value=True):
    """Sets the running status for the user:
    True if the user already has a running job. False if not."""
    return redis_pool.set('users.jobs.email.{}'.format(email), value)


def delete_email(email):
    """Deletes the user email from the database."""
    return bool(redis_pool.delete('users.jobs.email.{}'.format(email)))


def user_id_for_email(email):
    """Retrieves a random user_id per email.
    If the email already has a user_id stored, use it. If not, generate a new one."""
    user_id = redis_pool.get('users.ids.{}'.format(email))

    if not user_id:
        user_id = str(uuid4())
        redis_pool.set('users.ids.{}'.format(email), user_id)

    return user_id


def set_result_for_user_id(user_id, result_url):
    """Writes the result URL for the user id (run_id)."""
    return redis_pool.set('users.jobs.result_url.{}'.format(user_id), result_url)


def get_result_by_user_id(user_id):
    """Retrieve download url for user_id."""
    return redis_pool.get('users.jobs.result_url.{}'.format(user_id))

