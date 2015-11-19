#!/bin/bash

TIME=30
LISTENERS=3
TRANSPORT='ros'

DIR=./data/network
mkdir -p ${DIR}

clear
for PACKET in '1000'
do
    for RATE in '0.01' '1' '2' '3' '4' '5' '6' '7' '8' '9' '10' '11' '12' '13' '14' '15'
    do
        echo $TRANSPORT $PACKET $LISTENERS $RATE

        ./network.py $DIR                      \
                     --listeners $LISTENERS    \
                     --packet $PACKET          \
                     --rate $RATE              \
                     --transport $TRANSPORT    \
                     --time $TIME

        sleep 5
        echo ''
    done
    sleep 10
done

echo -e "\a"
