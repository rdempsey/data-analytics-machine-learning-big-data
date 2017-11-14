#!/bin/bash

# Update the OS
sudo apt-get update && sudo apt-get -y dist-upgrade

# Install compilers and build tools
sudo apt-get -y install build-essential
sudo apt install members

# Install Java
sudo apt-get -y install openjdk-8-jdk-headless
echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> ~/.profile