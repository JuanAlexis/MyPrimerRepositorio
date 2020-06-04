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
totalsByAge = rdd.reduceByKey(lambda  x , y: x + y)
#Como la funcion collections() no es soportada para el sort, usamos la accion countByValue() 
#para forzar a que se pueda ordenar por la clave, aunque el valor Count no lo imprimimos después. 
results = totalsByAge.countByValue()

sortedResults = collections.OrderedDict(sorted(results.items()))
for key, value in sortedResults.items():
    print("Edad: %s Amigos %i" % (key)) 
