#!/bin/bash

NODE_TYPE=$1

# Install Hadoop
cd /home/students/
wget http://mirror.stjschools.org/public/apache/hadoop/common/hadoop-2.7.4/hadoop-2.7.4.tar.gz
tar xvzf hadoop-2.7.4.tar.gz

# Install and configure Hadoop
sed -i "s|export JAVA_HOME=\${JAVA_HOME}|export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64|" /home/students/hadoop-2.7.4/etc/hadoop/hadoop-env.sh
yes | cp /home/ubuntu/configs/hadoop/all/core-site.xml /home/students/hadoop-2.7.4/etc/hadoop/core-site.xml

# Make a Hadoop data directory and set permissions
sudo mkdir -p /home/students/hadoop/hdfs/data
sudo chown -R ubuntu:students /home/students/hadoop/hdfs/data

if [[ $NODE_TYPE == 'name' ]]
then
  # Setup SSH config
  yes | cp /home/ubuntu/configs/ssh/config ~/.ssh/config
  # Setup Core Site
  yes | cp /home/ubuntu/configs/hadoop/all/core-site.xml /home/students/hadoop-2.7.4/etc/hadoop/core-site.xml
  # Setup HDFS properties
  yes | cp /home/ubuntu/configs/hadoop/namenode/hdfs-site.xml /home/students/hadoop-2.7.4/etc/hadoop/hdfs-site.xml
  # Setup MapReduce properties
  yes | cp /home/ubuntu/configs/hadoop/namenode/mapred-site.xml /home/students/hadoop-2.7.4/etc/hadoop/mapred-site.xml
  # Setup YARN properties
  yes | cp /home/ubuntu/configs/hadoop/namenode/yarn-site.xml /home/students/hadoop-2.7.4/etc/hadoop/yarn-site.xml
  # Setup master and slaves
  yes | cp /home/ubuntu/configs/hadoop/namenode/masters /home/students/hadoop-2.7.4/etc/hadoop/masters
  yes | cp /home/ubuntu/configs/hadoop/namenode/slaves /home/students/hadoop-2.7.4/etc/hadoop/slaves
elif [[ $NODE_TYPE == 'data' ]]
then
  # Setup Core Site
  yes | cp /home/ubuntu/configs/hadoop/all/core-site.xml /home/students/hadoop-2.7.4/etc/hadoop/core-site.xml
  # Setup HDFS properties
  yes | cp /home/ubuntu/configs/hadoop/datanodes/hdfs-site.xml /home/students/hadoop-2.7.4/etc/hadoop/hdfs-site.xml
fi

# Cleanup
rm /home/students/hadoop-2.7.4.tar.gz