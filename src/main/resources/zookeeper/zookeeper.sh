#!/usr/bin bash

bin/zkServer.sh start # start zookeeper, config file needed to setup

bin/zkCli.sh -server 127.0.0.1:2181 # connect to zookeeper

