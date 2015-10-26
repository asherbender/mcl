#!/bin/bash

# Set a unique IP address
ifconfig eth0 10.0.0.69

# Change socket buffer size
sysctl -w net.core.rmem_max=2097152
sysctl -w net.core.rmem_default=2097152

# Add multicast routes
ip route add 224.0.0.0/4 dev eth0

# Enable forwarding
sysctl -w net.ipv4.ip_forward=1
# Disable ignore ICMP broadcasts
sysctl -w net.ipv4.icmp_echo_ignore_broadcasts=0
