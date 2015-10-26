#!/bin/bash

# By default, the guest user is prohibited from connecting to the broker
# remotely; it can only connect over a loopback interface (i.e. localhost). This
# applies both to AMQP and to any other protocols enabled via plugins. Any other
# users you create will not (by default) be restricted in this way.
#
# Create a 'test' user for messaging across the network.
#
rabbitmqctl add_user test test
rabbitmqctl set_user_tags test administrator
rabbitmqctl set_permissions -p / test ".*" ".*" ".*"
