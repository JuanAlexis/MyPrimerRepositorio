#
import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("ContarPalabras")
sc = SparkContext(conf = conf)

RDD_Lines = sc.textFile("file:///CursoSpark/libro.txt")
words = RDD_Lines.flatMap (normalizeWords)
#wordcount = words.countByValue()
totalsByAge = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
cambio = totalsByAge.map(lambda x: (x[1],x[0]))
orden = cambio.sortByKey()

results = orden.collect()
for cantidad, palabra in results:
    #print(cantidad, palabra)

    cleanWord = palabra.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(cantidad))
