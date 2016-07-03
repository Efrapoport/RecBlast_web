import rq
from redis_pool import redis_pool
# Import worker functions
import worker

queue = rq.Queue(connection=redis_pool)

#
# Here we add calls to queue methods from the worker module
#
#

# Wrapper for worker function
def run_part_one(param):
    queue.enqueue(worker.part_one, param)
