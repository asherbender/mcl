#!/bin/bash

TIME=20
LISTENERS=3
TRANSPORT='mcl'

clear
for RATE in '0.01' '0.1' '1' '2' '3' '4' '5' '6' '7' '8'  '9'  '10' '15'
do
    FNAME=./data/network_${TRANSPORT}_${LISTENERS}_${RATE}.pkl
    echo $TRANSPORT $LISTENERS $RATE

    ./ping   --time $((TIME + 2)) --transport $TRANSPORT --rate $RATE           &
    sleep 0.25

    ./log $FNAME --time $TIME --transport $TRANSPORT
    sleep 5
    echo ''
done
