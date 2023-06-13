import time
from collections import Counter
from random import randint

import argparse

from more_itertools import chunked

from rabbits import RabbitConnector
from pubsub_datagen import Publication

from rabbits import logger


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--seconds",
        type=float,
        default=0.1,
        help="Number of seconds to keep generating",
    )
    args = parser.parse_args()
    num_seconds = args.seconds

    publisher = RabbitConnector(0)
    publisher.connect()
    publisher.consume()

    # Publish for 3 minutes

    time_left = num_seconds   # 0.1  # 60 * 3
    n_messages_per_loop = 3 * 10

    logger.info(f"Launching Publisher for {time_left} seconds.")

    end_time = time.perf_counter() + time_left + 1
    last_print = time.perf_counter()
    start_time = time.perf_counter()
    counter = Counter()
    num_brokers = 3
    while (cur_time := time.perf_counter()) < end_time:
        pubications = Publication.generate_n(n_messages_per_loop)
        for publication in pubications:
            broker = randint(1, num_brokers)
            publication.append(("_time_sent", time.time()))
            publisher.send(publication, broker)
            counter[broker] += 1
        if cur_time - last_print > 2:
            logger.info(f"Published {n_messages_per_loop} messages. Timeleft = {end_time - cur_time} seconds.")
            last_print = cur_time

    total_time = time.perf_counter() - start_time
    logger.info(f"Publisher finished in {total_time:.2f} s.")
    logger.info(f"Published {sum(counter.values())} messages in total.")
    logger.info(f"Published {counter} messages per broker.")
    logger.info(f"Published {sum(counter.values()) / total_time} messages per second.")


if __name__ == "__main__":
    main()
