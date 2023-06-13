import time
from collections import Counter
from random import randint

import multiprocessing
import logging

from more_itertools import chunked

from rabbits import Subscriber
from pubsub_datagen import Publication

from rabbits import get_logger
from rabbits import worker_configurer


SUBSCRIBER_IDS = (9001, 9002, 9003)


def make_subscriber(queue, subscriber_id: int):
    worker_configurer(queue)
    logger = logging.getLogger(__name__)  # Get the logger

    assert subscriber_id in SUBSCRIBER_IDS
    sub = Subscriber(subscriber_id)
    sub.connect()
    sub.consume()
    logger.info(f"Subscriber {subscriber_id} is alive.")
    while True:
        try:
            time.sleep(0.5)
            if randint(1, 100) > 92:
                sub.statistics['timedelta_mean'] = sum(sub.timedelta) / max(0.001, len(sub.timedelta))
                logger.info(f"Subscriber {subscriber_id} is alive with stats: {sub.statistics}")
        except KeyboardInterrupt:
            sub.statistics['timedelta_mean'] = sum(sub.timedelta) / max(0.001, len(sub.timedelta))
            logger.info(f"Subscriber {subscriber_id} exiting.")
            logger.info(f"Subscriber {subscriber_id} statistics: {sub.statistics}")
            break
    return sub


if __name__ == "__main__":
    # Launch brokers in separate processes

    # Get a manager for managing process-safe data types
    manager = multiprocessing.Manager()
    # Your queue to be shared among processes
    queue = manager.Queue(-1)
    # Get the logger and listener
    logger, listener = get_logger(queue)

    with multiprocessing.Pool(processes=3) as pool:
        try:
            pool.starmap(make_subscriber, [(queue, sid) for sid in SUBSCRIBER_IDS])
        except KeyboardInterrupt:
            time.sleep(1)
            logger.info("KeyboardInterrupt received. Shutting down all subscribers.")
            pool.terminate()
            pool.join()
    time.sleep(1)
    logger.info("All subscribers exited.")
    # Stop the listener process
    listener.stop()
