# projetCBD

This project perform the low rank "Matrix Factorization" of a user-ratings matrix.
The application is implemented in python and can be run
- with Ipython via the file "Projet-Optimisation.ipynb"
- on a spark-cluster via the shell file "run-app-on-cluster.sh"

# Requirements for Ipython
To run "Projet-Optimisation.ipynb", you need to install python with the following libraries
- numpy
- scipy
- matpotlib
- pyspark

# Requirement for the Spark Cluster
To run "run-app-on-cluster.sh", we use Docker to build images and containers for the master, the workers and the driver.
So in order to run the application on a cluster, you need to :
- install Docker on your computer (https://www.docker.com/)
- run "run-app-on-cluster.sh"
- monitor the progress of the application on http://localhost:8080

Please note that "run-app-on-cluster.sh" runs the scripts which are located in "spark-cluster" folder in the following order:
1. build.sh : to build the images for the master, the workers and the driver. Look at the dockerfiles for more information on the configuration of these images
2. start.sh : to start the application
3. stop.sh : to stop the application
