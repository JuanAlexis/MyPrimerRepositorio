from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("ContadorRatings")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///CursoSpark/ml-100k/u.data")

RDD = lines.map(lambda x: int(x.split()[1]))
RDD_create_value      = RDD.map(lambda x: (x, 1))
#simplificado: movies = lines.map(lambda x: (int(x.split()[1]), 1))
RDD_countByValue      = RDD_create_value.reduceByKey(lambda x, y: x + y)
RDD_exchange          = RDD_countByValue.map(lambda x: (x[1], x[0]))
RDD_Sorted            = RDD_exchange.sortByKey()

results = RDD_Sorted.collect()
for result in results:
    print(result)
