#Este programa imprime el numero de amigos para cada una de las edades
#Note que los resultados ahora estan ordenados.
from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("ContadorRatings")
sc = SparkContext(conf = conf)

def parseLine(linea):
    fields = linea.split(',')
    age = int(fields[2])
    amigos = int(fields[3])
    return (age,amigos)

lines = sc.textFile("file:///CursoSpark/amigos.csv")
rdd = lines.map(parseLine)
#Con esta función, se sumariza los valores (amigos) por la clave (edad)
RDD_totalsByAge = rdd.reduceByKey(lambda  x , y: x + y)
#Con esta función, se ordena el RDD por clave - aplicada por me
RDD_sortByKey = RDD_totalsByAge.sortByKey()

results = RDD_sortByKey.collect()
for Edad, NumeroAmigos in results:
    print("Edad: %s Amigos %i" % (Edad,NumeroAmigos))
