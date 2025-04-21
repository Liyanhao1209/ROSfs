# Start with the Ubuntu 20.04 base image
FROM ros:noetic

ENV TZ=Asia/Shanghai \
    DEBIAN_FRONTEND=noninteractive
# Set the locale to UTF-8
ENV LANG C.UTF-8

# Set the shell to bash
SHELL ["/bin/bash", "-c"]

# Install required packages for ROS and your own code
RUN apt-get update && apt-get install -y \
    curl \
    vim \
    gnupg2 \
    lsb-release \
    sudo \
    build-essential \
    iputils-ping \
    net-tools \
    cmake \
    git \
    libssl-dev \
    libusb-1.0-0-dev \
    pkg-config \
    python3-dev \
    python3-numpy \
    python3-pip \
    libzmq3-dev \
    tmux 

RUN pip3 install -i https://pypi.tuna.tsinghua.edu.cn/simple pyzmq

RUN sudo sh -c 'echo "deb http://packages.ros.org/ros/ubuntu $(lsb_release -sc) main" > /etc/apt/sources.list.d/ros-latest.list'
RUN curl -s https://raw.githubusercontent.com/ros/rosdistro/master/ros.asc | sudo apt-key add -

RUN apt-get install -y python3-rosdep python3-rosinstall-generator python3-vcstools python3-vcstool
RUN rosdep update

# Copy your code into the image
RUN mkdir /root/ros_catkin_ws
COPY . /root/ros_catkin_ws
WORKDIR /root/ros_catkin_ws
# install ros dependencies
RUN rosdep install --from-paths ./src --ignore-packages-from-source --rosdistro noetic -y
# build ros from source in docker 
RUN rm -rf build/
RUN /root/ros_catkin_ws/src/catkin/bin/catkin_make
RUN /bin/bash /root/ros_catkin_ws/devel/setup.bash

#CMD ['roscore']


