import rq
from redis_pool import redis_pool
# Import worker functions
import worker

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
    result = queue.enqueue(worker.run_recblast_web, values)
    return result

#
# # Wrapper for worker function
# def run_part_one(param):
#     queue.enqueue(worker.part_one, param)

