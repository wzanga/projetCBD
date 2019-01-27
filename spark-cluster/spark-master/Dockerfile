#Spark Master
FROM ubuntu:16.04

RUN apt-get update && apt-get install -y \
 wget \
 default-jdk \
 python2.7 \
 scala && cd /home && mkdir spark && cd spark && \
 wget https://archive.apache.org/dist/spark/spark-2.1.1/spark-2.1.1-bin-hadoop2.6.tgz && \
 tar -xvf spark-2.1.1-bin-hadoop2.6.tgz

ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-amd64
ENV SPARK_HOME /home/spark/spark-2.1.1-bin-hadoop2.6

RUN apt-get install -y python-pip && pip install --upgrade pip
RUN pip install numpy
RUN pip install scipy
RUN apt-get install -y python-tk
RUN pip install matplotlib

ENTRYPOINT ["/home/spark/spark-2.1.1-bin-hadoop2.6/bin/spark-class","org.apache.spark.deploy.master.Master"]
