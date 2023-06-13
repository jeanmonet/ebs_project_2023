#!/bin/bash

# stop the RabbitMQ nodes
for i in 3 2 0
do
    RABBITMQ_CONF_ENV_FILE=./rabbitmq${i}-env.conf rabbitmqctl stop
    sleep 2
done

# stop the RabbitMQ subscriber nodes
for i in 3 2 1
do
    RABBITMQ_CONF_ENV_FILE=./rabbitmq900${i}-env.conf rabbitmqctl stop
    sleep 2
done

# Closing master node
RABBITMQ_CONF_ENV_FILE=./rabbitmq1-env.conf rabbitmqctl stop

echo "RabbitMQ cluster stopped"

