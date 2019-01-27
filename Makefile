GIT 	= Makefile README.md
SRC1 	= Projet-Optimisation.ipynb ./data
SRC2	= ./spark-cluster/data ./spark-cluster/*.sh
SRC3	= ./spark-cluster/spark-datastore/Dockerfile
SRC4 	= ./spark-cluster/spark-master/Dockerfile
SRC5	= ./spark-cluster/spark-slave/Dockerfile
SRC6	= ./spark-cluster/spark-submit/Dockerfile

send:
	git add *
	git commit -m "Added and tested pyspark-cluster"
	git push

xport:
	docker save -o "$(pwd)" "$(IMAGENAME)"
