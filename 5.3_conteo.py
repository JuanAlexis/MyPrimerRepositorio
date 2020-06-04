#aplica el flatMap (todo el texto lo separa en tiras o filas distintas)
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("ContarPalabras")
sc = SparkContext(conf = conf)

RDD_Lines = sc.textFile("file:///CursoSpark/cuento.txt")
RDD_Split = RDD_Lines.flatMap (lambda x: x.split())
wordcount = RDD_Split.countByValue()

#results = RDD_Split.collect()
for result in wordcount.items():
     print(result)
