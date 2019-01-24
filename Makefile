SRC					=app.py
NAME				=bigdata
FOLDERPATH	='/Users/zangawilliams/projetCBD/'
IMAGENAME		='bigdata'
PATHBIN			=/anaconda3/pkgs/pyspark-2.4.0-py27_0/lib/python2.7/site-packages/pyspark/bin
PYTHON			=python2
RUN					=spark-submit
OPTION			=--master local[4]

send:
	git add *
	git commit -m "Application functional on pyspark local"
	git push

dbuild:
	docker build --tag=$(NAME) .

drun:
	docker run -p 4000:80 $(NAME)

dexport:
	docker save -o $(FOLDERPATH) $(IMAGENAME)

dprune:
	docker system prune -a

psubm:
	PYSPARK_PYTHON=$(PYTHON) $(PATHBIN)/$(RUN) $(SRC)

pclust:
	time $(PATHBIN)/$(RUN) $(OPTION) ./$(SRC)
