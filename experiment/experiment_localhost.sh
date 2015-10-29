#!/bin/bash

TIME=30

clear
for PACKET in '4000'              #'1000' '2000' '4000'
do
    DIR=./data/localhost/${PACKET}
    mkdir -p ${DIR}

    for TRANSPORT in 'lcm' 'mcl' 'zmq' 'rabbitmq'
    do
        for LISTENERS in '4'    #'1' '2' '4'
        do
            for RATE in '15'    #'0.01' '0.1' '1' '2' '3' '4' '5' '6' '7' '8'  '9'  '10' '15'
            do
                FNAME=${DIR}/${TRANSPORT}_${LISTENERS}_${RATE}.pkl
                echo $TRANSPORT $LISTENERS $RATE

                ./pong   --time $((TIME + 2))   \
                         --transport $TRANSPORT \
                         --listeners $LISTENERS &
                sleep 0.25

                ./ping   --time $((TIME + 2))   \
                         --transport $TRANSPORT \
                         --rate $RATE           \
                         --packet $PACKET       &
                sleep 0.25

                ./log $FNAME --time $TIME           \
                             --transport $TRANSPORT \
                             --listeners $LISTENERS
                sleep 5
                echo ''
            done
        done
    done
done
