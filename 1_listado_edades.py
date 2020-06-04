#Este programa imprime la columna edades del archivo
#A diferencia de imprimir varias columnas del listado, cuando es un sola columna no es necesario usar [0]
#para referenciar la posición única
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("ContadorRatings")
sc = SparkContext(conf = conf)

def parseLine(linea):
    fields = linea.split(',')
    edades = int(fields[2])
    return (edades)
    
RDD_lines = sc.textFile("file:///CursoSpark/amigos.csv")
RDD_edades = RDD_lines.map(parseLine)
#Como los RDD's no se imprimen directamente, debemos pasarlo a una colección.
results = RDD_edades.collect()
for result in results:
     print("Edad: %s " % (result))
     #Imprime-> Edad: 33

