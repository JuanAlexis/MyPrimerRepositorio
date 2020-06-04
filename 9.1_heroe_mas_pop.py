#En archivo de heroes, el IdHeroe es posicion 0 y el resto son otros hereos de su pelicula
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("ContadorRatings")
sc = SparkContext(conf = conf)
def parseline(line):
    fields = line.split()
    return (int(fields[0]), len(fields)-1)

def parseNames(line):
    fields = line.split()
    return (int(fields[0]), fields[1].encode("utf8"))

names = sc.textFile("file:///CursoSpark/marvel_names.txt")
namesRdd = names.map(parseNames)

lines = sc.textFile("file:///CursoSpark/Marvel-Graph.txt")
RDD                   = lines.map(parseline)
RDD_totalByHero       = RDD.reduceByKey(lambda x, y: x + y) 
RDD_exchange          = RDD_totalByHero.map(lambda x: (x[1], x[0])) 
mostPopular           = RDD_exchange.max()

MovieName       = namesRdd.lookup(mostPopular[1])[0]
print("Pelicula " + str(MovieName) +" fue vista " + str(mostPopular[0]))
    