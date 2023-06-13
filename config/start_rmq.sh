
#!/bin/bash

# start the first RabbitMQ node
CONF_ENV_FILE=./rabbitmq1-env.conf /opt/homebrew/opt/rabbitmq/sbin/rabbitmq-server -detached

# wait a bit to ensure the first node has started
sleep 5

# start the other nodes and join them to the cluster
for i in 2 3 0
do
    # CONF_ENV_FILE=./rabbitmq${i}-env.conf /opt/homebrew/opt/rabbitmq/sbin/rabbitmq-plugins disable rabbitmq_management
    # sleep 2
    CONF_ENV_FILE=./rabbitmq${i}-env.conf /opt/homebrew/opt/rabbitmq/sbin/rabbitmq-server -detached
    sleep 5
    CONF_ENV_FILE=./rabbitmq${i}-env.conf /opt/homebrew/opt/rabbitmq/sbin/rabbitmqctl stop_app
    sleep 2
    CONF_ENV_FILE=./rabbitmq${i}-env.conf /opt/homebrew/opt/rabbitmq/sbin/rabbitmqctl reset
    sleep 2
    CONF_ENV_FILE=./rabbitmq${i}-env.conf /opt/homebrew/opt/rabbitmq/sbin/rabbitmqctl join_cluster rabbit1@localhost
    sleep 2
    CONF_ENV_FILE=./rabbitmq${i}-env.conf /opt/homebrew/opt/rabbitmq/sbin/rabbitmqctl start_app
    sleep 5
done



# start the subscriber nodes and join them to the cluster
for i in 1 2 3
do
    # continue
    # CONF_ENV_FILE=./rabbitmq900${i}-env.conf /opt/homebrew/opt/rabbitmq/sbin/rabbitmq-plugins disable rabbitmq_management
    # sleep 2
    CONF_ENV_FILE=./rabbitmq900${i}-env.conf /opt/homebrew/opt/rabbitmq/sbin/rabbitmq-server -detached
    sleep 5
    CONF_ENV_FILE=./rabbitmq900${i}-env.conf /opt/homebrew/opt/rabbitmq/sbin/rabbitmqctl stop_app
    sleep 2
    CONF_ENV_FILE=./rabbitmq900${i}-env.conf /opt/homebrew/opt/rabbitmq/sbin/rabbitmqctl reset
    sleep 2
    CONF_ENV_FILE=./rabbitmq900${i}-env.conf /opt/homebrew/opt/rabbitmq/sbin/rabbitmqctl join_cluster rabbit1@localhost
    sleep 2
    CONF_ENV_FILE=./rabbitmq900${i}-env.conf /opt/homebrew/opt/rabbitmq/sbin/rabbitmqctl start_app
    sleep 5
done


# print cluster status
CONF_ENV_FILE=./rabbitmq1-env.conf /opt/homebrew/opt/rabbitmq/sbin/rabbitmqctl cluster_status

