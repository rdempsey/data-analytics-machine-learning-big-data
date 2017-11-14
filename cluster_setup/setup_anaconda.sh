#!/bin/bash

# Install Anaconda
cd /home/students
wget https://repo.continuum.io/archive/Anaconda3-4.4.0-Linux-x86_64.sh
chmod +x Anaconda3-4.4.0-Linux-x86_64.sh
bash /home/students/Anaconda3-4.4.0-Linux-x86_64.sh -b -p /home/students/anaconda

# Export the path so we can use it
export PATH="/home/students/anaconda/bin:$PATH"
sudo echo "export PATH=$PATH:/home/students/anaconda/bin" >> ~/.profile

# Create an Anaconda environment
conda create -y --name mlfbd python=3 pandas numpy matplotlib seaborn scikit-learn
source activate mlfbd

# Install additional libraries
pip install findspark jupyter psutil requests

# Give students full permissions on the folder
sudo chgrp -R students /home/students/anaconda
# sudo chmod -R 2775 /home/students/anaconda

# Cleanup
rm /home/students/Anaconda3-4.4.0-Linux-x86_64.sh