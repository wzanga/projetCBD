#!/bin/bash

set -e

cd ./spark-master/ && echo 'in spark-master' && docker build -t spark-master .
cd ../spark-slave/ && echo 'in spark-master' && docker build -t spark-slave .
cd ../spark-submit/ && echo 'in spark-master' && docker build -t spark-submit .
cd ../spark-datastore/ && echo 'in spark-master' && docker build -t spark-datastore .
