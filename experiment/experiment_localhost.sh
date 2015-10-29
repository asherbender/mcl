#!/bin/bash

TIME=10

clear
for PACKET in '1000' '2000' '4000'
do
    DIR=./data/localhost/${PACKET}
    mkdir -p ${DIR}

    for TRANSPORT in 'lcm' 'mcl' 'ros' 'zmq' 'rabbitmq'
    do
        for LISTENERS in '1' '2' '4'
        do
            for RATE in '0.01' '0.1' '1' '2' '3' '4' '5' '6' '7' '8' '9' '10' '15'
            do
                FNAME=${DIR}/${TRANSPORT}_${LISTENERS}_${RATE}.pkl
                echo $TRANSPORT $LISTENERS $RATE

                ./log $FNAME --time $((TIME + 4))    \
                             --transport $TRANSPORT  \
                             --listeners $LISTENERS  &
                sleep 0.5

                ./pong   --time $((TIME + 2))        \
                         --transport $TRANSPORT      \
                         --listeners $LISTENERS      &
                sleep 0.5

                ./ping   --time $TIME                \
                         --transport $TRANSPORT      \
                         --rate $RATE                \
                         --packet $PACKET

                while [ $(ps aux | grep -c 'usr/bin/python ./log') -gt 1 ]
                do
                    sleep 0.5
                done
                echo ''
            done
        done
    done
done
