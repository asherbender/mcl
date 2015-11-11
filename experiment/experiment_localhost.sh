#!/bin/bash

# A data collection time of 10 seconds will result in a 3hr experiment:
#
#     3x5x3x17x(10+5)/(60x60) = 3.1875
#
TIME=10

clear
for PACKET in '1000'             #'2000' '4000'
do
    DIR=./data/localhost/${PACKET}
    mkdir -p ${DIR}

    for TRANSPORT in 'rabbitmq'  #'lcm' 'mcl' 'ros' 'zmq' 
    do
        for LISTENERS in '4'     #'1' '2' '4'
        do
            for RATE in '0.01' '0.1' '1' '2' '3' '4' '5' '6' '7' '8' '9' '10' '11' '12' '13' '14' '15'
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
done
