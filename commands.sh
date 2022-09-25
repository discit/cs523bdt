#!/bin/bash

#For running this script, in terminal, cd to where this file is located and then run: bash ./commands.sh

clear
echo Creating directories and cleaning data in HDFS...
hadoop fs -mkdir -p /user/cloudera/hive_tables
hadoop fs -mkdir -p /user/cloudera/hive_tables/all_tweets
hadoop fs -rm -r /user/cloudera/hive_tables/all_tweets/*
hadoop fs -mkdir -p /user/cloudera/temp_tweets
hadoop fs -rm -r /user/cloudera/temp_tweets/*


echo Create table in Hive...
hive -f /home/cloudera/cs523/finalproject/createtable.hql;

echo Safe to run jars.
