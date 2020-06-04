#Este programa imprime el numero de personas para cada una de las edades
from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("ContadorRatings")
sc = SparkContext(conf = conf)

def parseLine(linea):
    fields = linea.split(',')
    age = int(fields[2])
    return (age)

RDD_lines = sc.textFile("file:///CursoSpark/amigos.csv")
RDD_edades = RDD_lines.map(parseLine)
#Ahora hacemos una acción para contar la cantidad de veces que existene en el RDD
results = RDD_edades.countByValue()
#Esta es la función para llevar a una lista los resultados de una acción
sortedResults = collections.OrderedDict(sorted(results.items()))
for key, value in sortedResults.items():
    print("Edad: %s Numero de Personas con esa edad: %i" % (key, value))
#Imprime -> Edad: 18 Numero de Personas con esa edad: 8

#Lo mismo que el anterior, solo que en vez de Key,Value, imprimimos todo el result
#for result in sortedResults.items():
#    print(result)
# Imprime (18, 8)

#Esta es la función para llevar a una lista los resultados de una acción, sin ordenar
#for result in results.items():
#    print(result)
#Imprime (18, 8)

#for result in RDD_results:
#    print(result)
#Imprime 33

#     print("Edad: %s Numero Amigos %i" % (result,results[result]))


