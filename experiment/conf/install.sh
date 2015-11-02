#!/bin/bash

# Define home.
USR_HOME='/home/ivssg'

# Install ntpdate for sync'ing clocks.
apt-get install ntpdate

#-------------------------------------------------------------------------------
# Install MCL
#-------------------------------------------------------------------------------
echo 'Updating MCL'
if [ ! -d $USR_HOME/mcl ]; then
    git clone git@loki:mcl $USR_HOME/mcl
fi
cd $USR_HOME/mcl
git checkout experiment
git pull
echo -e '\n\n'

#-------------------------------------------------------------------------------
# Install LCM
#-------------------------------------------------------------------------------
if [ ! -d $USR_HOME/lcm ]; then
    echo 'Installing LCM'
    apt-get install build-essential libglib2.0-dev python-dev
    git clone https://github.com/lcm-proj/lcm $USR_HOME/lcm
    cd $USR_HOME/lcm
    ./bootstrap.sh
    ./configure
    make
    make install

    # Generate LCM messages.
    lcm-gen -p $USR_HOME/mcl/experiment/conf/messages.lcm \
            --ppath $USR_HOME/mcl/experiment/common/

    echo -e '\n\n'
fi

#-------------------------------------------------------------------------------
# Install ZMQ
#-------------------------------------------------------------------------------
echo 'Installing ZMQ'
pip install pyzmq
echo -e '\n\n'

#-------------------------------------------------------------------------------
# Install RabbitMQ
#-------------------------------------------------------------------------------
echo 'Installing RabbitMQ'
apt-get install rabbitmq-server
pip install pika
echo -e '\n\n'

#-------------------------------------------------------------------------------
# Install ROS
#-------------------------------------------------------------------------------
# https://wiki.debian.org/DebianScience/Robotics/ROS

if [ ! -d $USR_HOME/ros ]; then
    echo 'Installing ROS'
    sh -c 'echo "deb http://sir.upc.edu/debian-robotics jessie-robotics main" > /etc/apt/sources.list.d/debian-robotics.list'
    apt-key adv --keyserver pgp.rediris.es --recv-keys 63DE76AC0B6779BF
    apt-get update
    apt-get install ros-desktop-full-depends

    mkdir $USR_HOME/ros
    cd $USR_HOME/ros
    rosinstall_generator ros_comm --rosdistro jade --deps --wet-only > jade-barebones-full-wet.rosinstall
    wstool init -j8 src jade-barebones-full-wet.rosinstall
    catkin_make_isolated --install
    echo -e '\n\n'
fi
