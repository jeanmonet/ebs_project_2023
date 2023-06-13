import time
from collections import Counter
from random import randint

from more_itertools import chunked

from rabbits import RabbitConnector
from pubsub_datagen import Publication

import logging
import multiprocessing

from rabbits import get_logger
from rabbits import worker_configurer


BROKER_IDS = (1, 2, 3)


def make_broker(queue, broker_id: int):
    worker_configurer(queue)
    logger = logging.getLogger(__name__)  # Get the logger

    assert broker_id in BROKER_IDS
    broker = RabbitConnector(broker_id)
    broker.connect()
    broker.consume()
    while True:
        try:
            time.sleep(0.5)
            if randint(1, 100) > 92:
                broker.statistics["num_filters"] = broker.router.num_filters
                broker.statistics['timedelta_unmatched_mean'] = sum(broker.timedelta_unmatched) / max(0.001, len(broker.timedelta_unmatched))
                logger.info(f"Broker {broker_id} is alive with stats: {broker.statistics}")
        except KeyboardInterrupt:
            broker.statistics["num_filters"] = broker.router.num_filters
            broker.statistics['timedelta_unmatched_mean'] = sum(broker.timedelta_unmatched) / max(0.001, len(broker.timedelta_unmatched))
            logger.info(f"Broker {broker_id} stats: {broker.statistics}")
            # broker.statistics['timedelta_unmatched'] = timedeltas
            break
    return broker


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
            pool.starmap(make_broker, [(queue, bid) for bid in BROKER_IDS])
        except KeyboardInterrupt:
            time.sleep(1)
            logger.info("KeyboardInterrupt received. Shutting down all brokers.")
            pool.terminate()
            pool.join()
    time.sleep(1)
    logger.info("All brokers exited.")
    # Stop the listener process
    listener.stop()
