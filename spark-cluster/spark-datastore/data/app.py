from pyspark import SparkContext
import sys

sc = SparkContext()
#lines = sc.textFile(sys.argv[1])
lines = sc.textFile("iliad100.txt")
word_counts = lines.flatMap(lambda line : line.split(' ')).map(lambda word : (word,1)).reduceByKey( lambda c1,c2 : c1+c2).collect()

for (word,count) in word_counts:
	print word.encode("utf8"), count
