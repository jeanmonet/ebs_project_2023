"""
$ python send_subscriptions.py -n 50

"""

import time
from collections import Counter
from random import randint

import argparse

import multiprocessing

from collections.abc import Callable

from more_itertools import chunked

from rabbits import Subscriber
from pubsub_datagen import SubGenSharder

from rabbits import logger


NUM_WORKERS = 3
NUM_SUBSCRIPTIONS = 100
BROKERS_RANGE = (1, 3)


def subscription_generators(
    num_subscriptions: int = NUM_SUBSCRIPTIONS,
) -> list[tuple[int, list[Callable]]]:
    sharder = SubGenSharder()

    field_percentages = {
        "city": 1,
        "temp": 0.9,
        "station_id": 0.,
        "wind": 0.6,
        "wind_direction": 0.6,
        "date": 0.4,
    }
    op_percentages = {
        ("city", "="): 1.,
        # ("station_id", "="): 1.,
        ("wind_direction", "="): 1.,
        ("wind", "="): 0.,
        ("wind", "!="): 0.,
        ("date", "="): 0.,
    }

    workers = sharder.shard(
        num_subscriptions,
        field_percentages,
        op_percentages,
        num_workers=NUM_WORKERS,
        return_params_only=False, )

    return zip(range(1, NUM_WORKERS + 1), workers)


def send_subscriptions(gen_worker_tup: tuple[int, list[Callable]]):
    subscriber_ix, gen_sub_worker = gen_worker_tup
    assert subscriber_ix in (1, 2, 3)
    subscriber = Subscriber(9000 + subscriber_ix)
    subscriber.connect()
    # subscriber.consume()
    for sub in gen_sub_worker():
        broker = randint(*BROKERS_RANGE)
        subscriber.send_subscription(sub, broker)
    logger.info(f"Subscriber {subscriber_ix} finished.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-n",
        "--num_subscriptions",
        type=int,
        default=NUM_SUBSCRIPTIONS,
        help="Number of subscriptions to generate.",
    )
    args = parser.parse_args()
    num_subscriptions = args.num_subscriptions

    start_time = time.perf_counter()
    logger.info("Launching Subscribers.")
    # Launch 3 subscribers in parallel processes
    with multiprocessing.Pool(processes=3) as pool:
        pool.map(send_subscriptions, subscription_generators(num_subscriptions))

    total_time = time.perf_counter() - start_time

    time.sleep(1)
    logger.info(f"{num_subscriptions} subscriptions sent in {total_time:.2f} s.")


if __name__ == "__main__":
    main()
