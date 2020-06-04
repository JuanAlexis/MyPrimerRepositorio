#Este programa imprime las columnas edades y amigos del archivo
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("ContadorRatings")
sc = SparkContext(conf = conf)

def parseLine(linea):
    fields = linea.split(',')
    edades = int(fields[2])
    amigos =  int(fields[3])
    return (edades,amigos)
    
RDD_lines = sc.textFile("file:///CursoSpark/amigos.csv")
RDD_edades_y_amigos = RDD_lines.map(parseLine)
#Como los RDD's no se imprimen directamente, entonces usamos la función collect()
results = RDD_edades_y_amigos.collect()
for col_Edad,col_Amigos in results:
    print("Edad: %s Amigos %i" % (col_Edad,col_Amigos))
#imprime->  Edad: 18 Amigos 194

#for result in results:
#    print("Edad: %s Numero Amigos %i" % (result [0],result [1]))
#imprime->  Edad: 18 Amigos 194

     #print(result)
     #imprime-> (33,385)

