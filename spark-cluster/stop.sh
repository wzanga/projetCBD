#!/bin/bash

set -e

docker stop $(docker ps -aq)
docker rm $(docker ps -aq)
