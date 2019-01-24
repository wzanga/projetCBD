#Use an official Python runtime as a parent image
FROM python:2.7-slim

#Set the working directory to /app
WORKDIR /app

#Copy the current directory content into the container at /app
COPY . /app

#Install any needed packages
RUN pip install numpy
RUN pip install scipy
RUN pip install pyspark

#Make port 80 available to the world outside this container
EXPOSE 80

#Define environment variable
ENV NAME World

#Run app.py when the container launches
CMD ["python","app.py"]
