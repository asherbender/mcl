#!/bin/bash

# Note if the IP is reconfigured here, ./ntp_client.conf will need to be
# modified.
SERVER_IP=100

#-------------------------------------------------------------------------------
# Get IP address from hostname
#-------------------------------------------------------------------------------

# Set IP address of server.
if [[ $HOSTNAME == 'ivssg' ]]; then
    NUMBER=$SERVER_IP

# Set IP address of clients.
elif [[ $HOSTNAME == *'ivssg'* ]]; then
    NUMBER=$(echo $HOSTNAME | sed 's/[^0-9]*//g')

# Assume host is server.
else
    NUMBER=$SERVER_IP
fi

#-------------------------------------------------------------------------------
# Configure UDP multicast and buffer size
#-------------------------------------------------------------------------------
echo Setting: eth0 to 10.0.0.${NUMBER}
echo

# Set a unique IP address.
ifconfig eth0 10.0.0.$NUMBER

# Change socket buffer size.
sysctl -w net.core.rmem_max=2097152
sysctl -w net.core.rmem_default=2097152

# Add multicast routes.
ip route add 224.0.0.0/4 dev eth0

# Enable forwarding and disable ignore ICMP broadcasts.
sysctl -w net.ipv4.ip_forward=1
sysctl -w net.ipv4.icmp_echo_ignore_broadcasts=0

#-------------------------------------------------------------------------------
# Configure NTP server
#-------------------------------------------------------------------------------

# Save original NTP configuration.
if [ ! -f /etc/ntp.conf.bak ]; then
    mv /etc/ntp.conf  /etc/ntp.conf.bak
fi

# Kill dhclient.
killall dhclient

# Stop NTP server.
service ntp stop

# Set NTP configuration of server.
if [[ $HOSTNAME == 'ivssg' ]]; then
    cp ./ntp_server.conf /etc/ntp.conf

# Set NTP configuration of clients.
elif [[ $HOSTNAME == *'ivssg'* ]]; then
    cp ./ntp_client.conf /etc/ntp.conf
    ntpdate 10.0.0.$SERVER_IP

# Assume host is server.
else
    cp ./ntp_server.conf /etc/ntp.conf
fi

# Restart NTP server with new configuration.
sudo ntpd -c /etc/ntp.conf

#-------------------------------------------------------------------------------
# Create test user for RabbitMQ
#-------------------------------------------------------------------------------
#
# By default, the guest user is prohibited from connecting to the broker
# remotely; it can only connect over a loopback interface (i.e. localhost). This
# applies both to AMQP and to any other protocols enabled via plugins. Any other
# users you create will not (by default) be restricted in this way.
#
# Create a 'test' user for messaging across the network.
#
echo
rabbitmqctl add_user test test
rabbitmqctl set_user_tags test administrator
rabbitmqctl set_permissions -p / test ".*" ".*" ".*"

#-------------------------------------------------------------------------------
# Echo configuration
#-------------------------------------------------------------------------------
echo
ifconfig eth0
echo
ntpq -p
