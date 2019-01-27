send:
	git add *
	git commit -m "Added and tested pyspark-cluster"
	git push

xport:
	docker save -o "$(pwd)" $(IMAGENAME)
