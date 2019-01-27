#!/bin/bash

set -e

cd ./spark-master/ && echo 'in spark-master' && docker build -t brunocf/spark-master .
cd ../spark-slave/ && echo 'in spark-master' && docker build -t brunocf/spark-slave .
cd ../spark-submit/ && echo 'in spark-master' && docker build -t brunocf/spark-submit .
cd ../spark-datastore/ && echo 'in spark-master' && docker build -t brunocf/spark-datastore .
