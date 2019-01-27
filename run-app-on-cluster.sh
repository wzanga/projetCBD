#!/bin/bash

set -e

cd ./spark-cluster/ && echo 'In spark-cluster'
sh ./build.sh
sh ./start.sh
sh ./stop.sh
