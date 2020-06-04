#Este programa imprime el numero de amigos para cada una de las edades
#Note que los resultados no estan ordenados.
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
totalsByAge = rdd.reduceByKey(lambda  x , y: x + y)
#Como los RDD's no se imprimen directamente, usamos la función collect()
results = totalsByAge.collect()
for Edad,NumeroAmigos in results:
    print("Edad: %s Numero Amigos %i" % (Edad,NumeroAmigos))
#Imprime-> Edad: 18 Numero Amigos 2747

#for result in results:
#    print(result)
#Imprime-> (18, 2747)