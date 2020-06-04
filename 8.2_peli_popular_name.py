from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("ContadorRatings")
sc = SparkContext(conf = conf)

def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.ITEM") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

nameDict = sc.broadcast(loadMovieNames())
lines = sc.textFile("file:///CursoSpark/ml-100k/u.data")

RDD = lines.map(lambda x: int(x.split()[1]))
RDD_create_value      = RDD.map(lambda x: (x, 1))
RDD_countByValue      = RDD_create_value.reduceByKey(lambda x, y: x + y)
RDD_exchange          = RDD_countByValue.map(lambda x: (x[1], x[0]))
RDD_Sorted            = RDD_exchange.sortByKey()
RDD_SortedWithNames   = RDD_Sorted.map(lambda x : (nameDict.value[x[1]], x[0]))

results = RDD_SortedWithNames.collect()
for result in results:
    print(result)
