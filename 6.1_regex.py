#aplica el flatMap (todo el texto lo separa en tiras o filas distintas)
import re
from pyspark import SparkConf, SparkContext
#Elimina las ",",":","&",etc
def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("ContarPalabras")
sc = SparkContext(conf = conf)

RDD_Lines = sc.textFile("file:///CursoSpark/cuento.txt")
RDD_Split = RDD_Lines.flatMap (normalizeWords)
#wordcount = words.countByValue()

results = RDD_Split.collect()
for result in results:
    #Esta parte sirve para no mostrar los caracteres especiales como æ
    cleanWord = result.encode('ascii', 'ignore')
    if (cleanWord):
     print(result)
