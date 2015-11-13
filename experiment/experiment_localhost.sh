#!/bin/bash
clear

TIME=10

# A data collection time of 10 seconds will result in:
#
#     1x5x3x16x10/(60x60) = 0.666 hrs
#
PACKET=1000
DIR=./data/localhost/${PACKET}
mkdir -p ${DIR}
for TRANSPORT in 'lcm' 'mcl' 'rabbitmq' 'ros' 'zmq'
do
    for LISTENERS in '1' '3' '6'
    do
        for RATE in '0.01' '1' '2' '3' '4' '5' '6' '7' '8' '9' '10' '11' '12' '13' '14' '15'
        do
            FNAME=${DIR}/${TRANSPORT}_${LISTENERS}_${RATE}.pkl
            echo $TRANSPORT $PACKET $LISTENERS $RATE

            ./localhost.py $FNAME                    \
                           --listeners $LISTENERS    \
                           --packet $PACKET          \
                           --rate $RATE              \
                           --transport $TRANSPORT    \
                           --time $TIME

            echo ''
        done
    done
done


# A data collection time of 10 seconds will result in:
#
#     1x5x4x16x10/(60x60) = 0.888 hrs
#
LISTENERS=3
for TRANSPORT in 'lcm' 'mcl' 'rabbitmq' 'ros' 'zmq'
do
    for PACKET in '500' '1500' '3000' '6000'
    do
        DIR=./data/localhost/${PACKET}
        mkdir -p ${DIR}
        for RATE in '0.01' '1' '2' '3' '4' '5' '6' '7' '8' '9' '10' '11' '12' '13' '14' '15'
        do
            FNAME=${DIR}/${TRANSPORT}_${LISTENERS}_${RATE}.pkl
            echo $TRANSPORT $PACKET $LISTENERS $RATE

            ./localhost.py $FNAME                    \
                           --listeners $LISTENERS    \
                           --packet $PACKET          \
                           --rate $RATE              \
                           --transport $TRANSPORT    \
                           --time $TIME

            echo ''
        done
    done
done
