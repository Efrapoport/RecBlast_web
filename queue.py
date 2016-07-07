import rq
from redis_pool import redis_pool
# Import worker functions
import worker
import users

queue = rq.Queue(connection=redis_pool)

#
# Here we add calls to queue methods from the worker module
#


# # Wrapper for worker function
# def run_email_func(app_contact_email, run_name, run_id, email_string):
#     result = queue.enqueue(worker.email_func, app_contact_email, run_name, run_id, email_string)
#     return result


# Wrapper for worker function
def run_recblast_on_worker(values):
    try:
        result = queue.enqueue(worker.run_recblast_web, values, timeout=1800)
        return result
    except Exception, e:
        users.delete_email(values[10])
        users.delete_user_id_for_email(values[10])
        print("Unknown error: {}\nDeleting email.".format(e))
        raise e

#
# # Wrapper for worker function
# def run_part_one(param):
#     queue.enqueue(worker.part_one, param)

