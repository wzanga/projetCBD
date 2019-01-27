#!/bin/bash

set -e

docker create -v "$(pwd)"/data:/data --name spark-datastore spark-datastore
docker run -d -p 8080:8080 -p 7077:7077 --volumes-from spark-datastore --name master spark-master
docker run -d --link master:master --volumes-from spark-datastore spark-slave
docker run -d --link master:master --volumes-from spark-datastore spark-slave
docker run -d --link master:master --volumes-from spark-datastore spark-slave
docker run --rm -it --link master:master --volumes-from spark-datastore spark-submit spark-submit --master spark://172.17.0.2:7077 /data/app.py
