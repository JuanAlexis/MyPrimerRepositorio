import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("ContarPalabras")
sc = SparkContext(conf = conf)

RDD = sc.textFile("file:///CursoSpark/libro.txt")
words = RDD.flatMap(normalizeWords)

RDD_crea_value      = words.map(lambda x: (x, 1))
RDD_total_word      = RDD_crea_value.reduceByKey(lambda x, y: x + y)
RDD_exchange        = RDD_total_word.map(lambda x: (x[1], x[0]))
RDD_Sorted          = RDD_exchange.sortByKey()
#También se puede hacer lo siguiente para ahorrar variables y lineas
#wordCountsSorted = words.map(lambda x: (x, 1)). RDD_crea_value.reduceByKey(lambda x, y: x + y).RDD_total_word.map(lambda x: (x[1], x[0])).sortByKey()

results = RDD_Sorted.collect()
#Esta es otra forma de trabajar el par clave valor
for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
