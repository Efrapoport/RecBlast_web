import rq
from redis_pool import redis_pool

queue = rq.Queue(connection=redis_pool)

from worker import run_test_loop


def run_part_one(param):
    queue.enqueue(run_test_loop, param)
