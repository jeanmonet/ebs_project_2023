from rich import print as printr
import random
from itertools import chain
from collections import Counter
import time

import logging
from logging.handlers import QueueHandler, QueueListener

from typing import Literal, Any, Union

import threading
import pickle
import json
import msgpack

import pika
import pandas as pd


logger = logging.getLogger("root")
logger.setLevel(logging.INFO)
# print messages to current terminal
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
logger.addHandler(ch)


# ThreadLock = threading.Lock()


conn_config = {
    0: ("rabbit0", "localhost", 5773),     # publisher
    1: ("rabbit1", "localhost", 5672),
    2: ("rabbit2", "localhost", 5673),
    3: ("rabbit3", "localhost", 5674),
}

sub_conn_config = {
    9001: ("rabbit9001", "localhost", 15672),
    9002: ("rabbit9002", "localhost", 15673),
    9003: ("rabbit9003", "localhost", 15674),
}


def serialize_message(message: list[tuple]) -> str:
    return pickle.dumps(message)


def deserialize_message(message: bytes) -> dict:
    return pickle.loads(message)


class FilterRoutingTable:
    def __init__(self):
        self.filters = {}  # type: dict[frozenset[tuple[str, str, Any]], list[tuple[str, str, Union[int, float, str]]]]
        self.filter2subscribers = {}  # type: dict[frozenset[tuple[str, str, Union[int, float, str]]], list[int]]
        self.num_filters = 0

    def add_filter(self, subscriber, filter):
        filter_frozen = frozenset(filter)
        if subscribers := self.filter2subscribers.get(filter_frozen):
            if subscriber not in subscribers:
                self.filter2subscribers[filter_frozen].append(subscriber)
            return
        self.filters.setdefault(frozenset(key for key, *_ in filter), []).append(filter)
        self.filter2subscribers.setdefault(filter_frozen, []).append(subscriber)
        self.num_filters += 1

    def match(self, notification) -> set[int]:
        matched_subscribers = set()
        if notification[-1][0] == "senders":
            notification = notification[:-1]
        if notification[-1][0] == "_time_sent":
            notification = notification[:-1]
        notification = sorted(notification[:-1])
        notification_keys = set(key for key, *_ in notification)
        for filter_keys, filters in self.filters.items():
            if filter_keys.issubset(notification_keys):
                # logger.info(f"Filter keys {filter_keys} a subset of notification keys {notification_keys}")
                for filter in filters:
                    if self._filter_to_notif_match(filter, notification):
                        # logger.info(f"Filter {filter} a MATCH for notification {notification}")
                        matched_subscribers.update(self.filter2subscribers[frozenset(filter)])
            else:
                # logger.info(f"Filter keys {filter_keys} not a subset of notification keys {notification_keys}")
                pass
        return matched_subscribers

    def _filter_to_notif_match(self, filters: list[tuple], notification: list[tuple]) -> bool:
        last_ix = 0    # filters & notification are sorted alphabetically by key
        for notification_key, notification_value in notification:
            for ix, (filter_key, filter_operator, filter_value) in enumerate(filters[last_ix:]):
                if notification_key == filter_key:
                    # logger.info(f"Matched {notification_key} to {filter_key}")
                    last_ix = ix + 1   # move to next filter key for next notification key
                    if filter_operator == "=" and str(notification_value) != str(filter_value):
                        return False
                    elif filter_operator == "!=" and str(notification_value) == str(filter_value):
                        return False
                    elif filter_operator == ">=" and float(notification_value) < float(filter_value):
                        return False
                    elif filter_operator == "<" and float(notification_value) >= float(filter_value):
                        return False
                    elif filter_operator == ">=" and float(notification_value) < float(filter_value):
                        return False
                    elif filter_operator == ">" and float(notification_value) <= float(filter_value):
                        return False
                    # Subfilter matched
                    break   # move to next notification key
        return True

    def __repr__(self):
        return f"{self.__class__.__name__}(num_filters={self.num_filters})"


class RabbitConnector:
    brokers = sorted(conn_config.keys())    # sorted list of broker indices
    conn_config = conn_config

    def __init__(self, rabbit: int) -> None:
        self.node, self.host, self.port = self.__class__.conn_config[rabbit]
        self.queue = f"broker{rabbit}"
        self.rabbit_ix = rabbit
        self.exchange_name = f"exchange{rabbit}"
        self.exchange_type = "fanout"

        self._should_stop_consumer = False
        self.consumer_tag = None
        self.router = FilterRoutingTable()

        self.statistics = {
            "num_messages": 0,
            "num_matched": 0,
            "num_unmatched": 0,
        }
        self.timedelta_unmatched = []

    def connect(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, port=self.port))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue, durable=False)
        # Declare the exchange on the broker
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type=self.exchange_type)
        # Bind the queue to the exchange
        self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue)

        # For the consumer thread
        self.consumer_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, port=self.port))
        self.consumer_channel = self.consumer_connection.channel()
        # self.consumer_channel.exchange_declare(exchange=self.exchange_name, exchange_type=self.exchange_type)
        # self.consumer_channel.queue_bind(exchange=self.exchange_name, queue=self.queue)

    def close(self):
        try:
            self.stop_consuming()
        except Exception as e:
            logger.error(e)
        try:
            self.connection.close()
        except AttributeError:
            logger.warning(f"Connection at broker{self.rabbit_ix} not yet established.")
            pass

    def send(self, message: list[tuple], rabbit_ix: int):
        exchange_name = f"exchange{rabbit_ix}"
        message.append(("senders", (self.rabbit_ix, )))
        # with ThreadLock:
        serialized = serialize_message(message)
        # logger.info(f"Sending {serialized!r} to {exchange_name}")
        self.channel.basic_publish(exchange=exchange_name, routing_key="", body=serialized)
        # logger.info(f"SENT {message!r} to {exchange_name}")

    def subscribe(self, filter: list[tuple], sender: int):
        self.router.add_filter(sender, filter)

    def consume(self, auto_ack=True):
        def callback(ch, method, properties, body):
            body = deserialize_message(body)
            # logger.info(f"Received on {self.queue}: {body}")
            _key, senders = body.pop()
            if _key != "senders":
                logger.error(f"Expected 'senders' key, got ({_key}, {senders})")
            assert _key == "senders", f"Expected sender key, got ({_key}, {senders})"

            last_sender = senders[-1]
            body.append(("senders", (*senders, self.rabbit_ix)))

            serialized_body = serialize_message(body)

            if body[-2][0] == "__SUBSCRIPTION__":
                # Add the subscription to the router
                self.subscribe(body[:-2], last_sender)
                # logger.info(f"Added subscription to {self.rabbit_ix} from {senders}: {body[:-2]}")
                # Propagate the subscription to all other brokers (except the sender) (simple routing) (non-circular)
                # Assume linear topology:   1 - 2 - 3
                possible_destinations = set(
                    self.__class__.brokers[
                        max(1, self.rabbit_ix - 1):self.rabbit_ix] + self.__class__.brokers[self.rabbit_ix + 1:self.rabbit_ix + 2])
                # logger.info(f"Possible destinations from {self.rabbit_ix}: {possible_destinations}")
                for i, destination in enumerate(possible_destinations - set(body[-1][1])):
                    if destination not in senders:
                        # Send the message to the destination exchange
                        # with ThreadLock:
                        exchange_name = f"exchange{destination}"
                        ch.basic_publish(exchange=exchange_name, routing_key="", body=serialized_body)
                        # self.consumer_channel.basic_publish(exchange=exchange_name, routing_key="", body=serialized_body)
                        # logger.info(f"Sent from {self.rabbit_ix} to {destination} {body}")
                return

            self.statistics["num_messages"] += 1
            # Get the destination for the message
            possible_destinations = self.router.match(body)
            destinations = possible_destinations - set(senders)
            if possible_destinations:
                # logger.info(f"Matched {self.rabbit_ix} to {destinations=} (from {possible_destinations=}): {body}")
                pass
            for destination in destinations:
                exchange_name = f"exchange{destination}"
                # Send the message to the destination exchange
                # with ThreadLock:
                # self.consumer_channel.basic_publish(exchange=exchange_name, routing_key="", body=serialized_body)
                ch.basic_publish(exchange=exchange_name, routing_key="", body=serialized_body)
                # logger.info(f"Sent from {self.rabbit_ix} to {destination}: {body}")
                self.statistics["num_matched"] += 1
                return
            # logger.warning(f"No destination found for {body}")
            self.statistics["num_unmatched"] += 1
            if "_time_sent" == body[-2][0]:
                self.timedelta_unmatched.append(time.time() - body[-2][-1])
            else:
                logger.warning(f"Before-last key in {body} is not '_time_sent'")

        # Start consuming in a new thread
        threading.Thread(target=self._consume, args=(callback, auto_ack), daemon=True).start()

    def _consume(self, callback, auto_ack):
        #with ThreadLock:
        self.consumer_tag = self.consumer_channel.basic_consume(queue=self.queue, on_message_callback=callback, auto_ack=auto_ack)
        logger.info(f"Starting consumption on {self.queue}")
        while not self._should_stop_consumer:
            self.consumer_connection.process_data_events()
        logger.info(f"Stopped consumption on {self.queue}")

    def stop_consuming(self):
        self._should_stop_consumer = True
        if self.consumer_tag is not None:
            # with ThreadLock:
            self.consumer_channel.basic_cancel(self.consumer_tag)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        self.close()

    def __repr__(self):
        return f"RabbitConnector(node={self.node}, host={self.host}, port={self.port}, queue={self.queue})"


class Subscriber(RabbitConnector):
    brokers = sorted(conn_config.keys())
    conn_config = sub_conn_config

    def __init__(self, rabbit: int) -> None:
        super().__init__(rabbit)
        self.statistics = {
            "num_messages": 0,
            "routes": Counter(),
        }
        self.timedelta = []

    def consume(self, auto_ack=True):
        def callback(ch, method, properties, body):
            body = deserialize_message(body)
            _key, senders = body.pop()
            assert _key == "senders", f"Expected sender key, got ({_key}, {senders})"
            # Run statistics on the message
            self.statistics["num_messages"] += 1
            self.statistics["routes"][senders] += 1
            if "_time_sent" == body[-1][0]:
                self.timedelta.append(time.time() - body[-1][-1])
            else:
                logger.warning(f"Last key in {body} is not '_time_sent'")

            if self.statistics["num_messages"] % 1000 == 0:
                logger.info(f"Subscriber received {body} on {self.queue}")
                self.statistics['timedelta_mean'] = sum(self.timedelta) / max(0.001, len(self.timedelta))
                logger.info(f"Subscriber statistics: {self.statistics}")

        # Start consuming in a new thread
        threading.Thread(target=self._consume, args=(callback, auto_ack), daemon=True).start()

    def send_subscription(self, message: list[tuple], rabbit_ix: int):
        message.append(("__SUBSCRIPTION__", ))
        self.send(message, rabbit_ix)


# ---- HELPERS ---


def worker_configurer(queue):
    h = QueueHandler(queue)  # Just the one handler needed
    root = logging.getLogger()
    root.addHandler(h)
    # send all messages, for demo; no other level or filter logic applied.
    root.setLevel(logging.DEBUG)

def listener_configurer():
    root = logging.getLogger()
    h = logging.StreamHandler()
    f = logging.Formatter('%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s')
    h.setFormatter(f)
    root.addHandler(h)

def listener_process(queue, configurer):
    configurer()
    listener = QueueListener(queue)
    listener.start()
    return listener

def get_logger(queue):
    listener = listener_process(queue, listener_configurer)
    worker_configurer(queue)
    return logging.getLogger(), listener
