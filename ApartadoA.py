from pyspark import SparkConf, SparkContext
from pyspark import SQLContext
from pyspark.sql.functions import udf
import string


conf = SparkConf().setAppName('MovieRating')
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferschema", "true").load("ratings.csv")

df2 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferschema", "true").load("movies.csv")

rddmovies = df2.rdd
rddratings = df.rdd

suma = rddratings.map(lambda x: (x[1],x[2]))
count = suma.map(lambda x: (x[0],1))

suma = suma.reduceByKey(lambda a, b: float(a) + float(b))
count = count.reduceByKey(lambda a, b: a + b)

average = suma.join(count)
average = average.map(lambda x: (x[0], x[1][0]/x[1][1]))

moviesWithAverage = average.join(rddmovies)

moviesTable = moviesWithAverage.map(lambda x: (x[1][1], x[1][0]))

rddfinal = moviesTable.map(lambda x: (x[0][len(x[0])-5]+x[0][len(x[0])-4]+x[0][len(x[0])-3]+x[0][len(x[0])-2],x[0],x[1] if 5 <= len(x[0]) else x[0],x[0],x[1]))
rddfinal = rddfinal.filter(lambda x: x[0].isdigit())
rddfinal = rddfinal.map(lambda x: (x[0].encode("ascii", "ignore"), (x[1].encode("ascii", "ignore"),x[2])))
rddfinal = rddfinal.reduceByKey(lambda a,b: ((a[0], a[1]) if a[1] > b[1] else ((a [0] + "; " + b[0], b[1]) if a[1] == b[1] else (b[0], b[1])))).sortByKey()

rddfinal.saveAsTextFile("outputApartadoA.txt")
