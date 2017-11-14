#!/bin/bash

# Install Scala
sudo apt-get -y install scala

# Install and configure Spark
cd /home/students
wget https://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz
tar xvzf spark-2.2.0-bin-hadoop2.7.tgz
mv spark-2.2.0-bin-hadoop2.7 spark-2.2.0
yes | cp /home/ubuntu/configs/spark/all/spark-env.sh /home/students/spark-2.2.0/conf/spark-env.sh
yes | cp /home/ubuntu/configs/spark/master/slaves /home/students/spark-2.2.0/conf/slaves

# Add the variables to the Ubuntu user
echo "export SPARK_HOME=/home/students/spark-2.2.0" >> ~/.profile
echo "export PATH=$PATH:$SPARK_HOME/bin" >> ~/.profile

# Export right now so we can use the variable
export SPARK_HOME=/home/students/spark-2.2.0

# Set the permissions on the directory
sudo chgrp -R students $SPARK_HOME
sudo chmod -R 2775 $SPARK_HOME

# Cleanup
rm /home/students/spark-2.2.0-bin-hadoop2.7.tgz