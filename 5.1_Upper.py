#Convierte en mayúsculas cada uno de los reglores tal cual están en el texto
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("ContarPalabras")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///CursoSpark/cuento.txt")
fullcaps=lines.map (lambda x: x.upper())

results = fullcaps.collect()
for result in results:
     print(result)
