#!/bin/bash

/home/students/hadoop-2.7.4/sbin/mr-jobhistory-daemon.sh stop historyserver
/home/students/hadoop-2.7.4/sbin/stop-yarn.sh
/home/students/hadoop-2.7.4/sbin/stop-dfs.sh
