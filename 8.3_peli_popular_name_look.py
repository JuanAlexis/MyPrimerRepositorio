#Vista de peliculas ordenadas asecendentemente con descripción.
#Un UserId -posicion 0- puede hacer rate de una o varias peliculas
#Una IddePelicula -posicion 1- puede ser ratiada por uno o varios usuarios. 

from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("ContadorRatings")
sc = SparkContext(conf = conf)
#Funcion que entrega el nombre de la palicula para un IddePelicula
def parseNames(line):
    fields = line.split('|')
    return (int(fields[0]), fields[1].encode("utf8"))

names = sc.textFile("file:///CursoSpark/ml-100k/u.item")
namesRdd = names.map(parseNames)

lines = sc.textFile("file:///CursoSpark/ml-100k/u.data")
RDD                   = lines.map(lambda x: int(x.split()[1])) #Obtiene IddePelicula
RDD_create_value      = RDD.map(lambda x: (x, 1)) #Agrega el valor 1 para formar la (clave, valor)
RDD_countByValue      = RDD_create_value.reduceByKey(lambda x, y: x + y) #(suma el valor)
RDD_exchange          = RDD_countByValue.map(lambda x: (x[1], x[0])) #Invierte (clave, valor) por (valor,clave)
RDD_SortedWithNames   = RDD_exchange.sortByKey(False) #De esta forma poder aplicar el sort por el valor

results = RDD_SortedWithNames.collect()
for result in results:
    #Como ahora se tiene los datos en una lista (total vistas,IddePelicula), entonces se obtiene 
    # el nombre de la pelicula en otro archivo con esta función:
    MovieName       = namesRdd.lookup(result[1])[0]
    #Esta funcion toma mas tiempo imprimir todo
    print("Pelicula " + str(MovieName) +" fue vista " + str(result[0]))
    