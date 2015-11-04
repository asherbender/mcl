#!/bin/bash

TIME=10
LISTENERS=3
TRANSPORT='mcl'

clear
for PACKET in '4000' '2000' '1000'
do
    DIR=./data/network/${PACKET}
    mkdir -p ${DIR}

    for RATE in '0.01' '0.1' '1' '2' '3' '4' '5' '6' '7' '8' '9' '10' '11' '12' '13' '14' '15'
    do
        FNAME=${DIR}/${TRANSPORT}_${LISTENERS}_${RATE}.pkl
        echo $TRANSPORT $PACKET $LISTENERS $RATE

        ./network.py $FNAME                    \
                     --listeners $LISTENERS    \
                     --packet $PACKET          \
                     --rate $RATE              \
                     --transport $TRANSPORT    \
                     --time $TIME

        sleep 3
        echo ''
    done
done
