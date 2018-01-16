from pyspark import SparkConf, SparkContext
from pyspark import SQLContext
import string
import datetime

conf = SparkConf().setAppName('Project')
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)


meses = ['Enero','Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']

df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferschema", "true").load("ratings.csv")
df2 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferschema", "true").load("movies.csv")

dfFinal = df2.join(df, ['movieId'], "outer") #unimos por el 'movieId' ambas tablas
rdd = dfFinal.rdd

rdd = rdd.filter(lambda x: x[5] is not None) #comprobamos que tenga fecha

#tenemos el titulo-generos-rating-fecha
rdd = rdd.map(lambda x: (x[1], x[2], x[4], datetime.datetime.fromtimestamp(x[5]).strftime('%Y-%m-%d %H:%M:%S')))


rddfinal = rdd.map(lambda x: (int(str(x[3]).split('-')[0]), x[1], x[2], meses[int(str(x[3]).split('-')[1]) - 1]))#rdd con ANYO-GENERO-RATING-MES

counter = rddfinal.map(lambda x: ((x[0], x[3], x[1]), 1)) #ponemos un 1 a todos los elementos
sumatorio = rddfinal.map(lambda x: ((x[0], x[3], x[1]), x[2])) #ponemos el rating a los elementos

counter = counter.reduceByKey(lambda a, b: a + b)
sumatorio = sumatorio.reduceByKey(lambda a, b: float(a) + float(b))

average = sumatorio.join(counter) #juntamos ambas tablas para hacer la media

average = average.map(lambda x: (x[0], x[1][0]/x[1][1])) #(ANYO, MES, GENERO), MEDIA

average = average.map(lambda x: ((x[0][0], x[0][1]), (x[0][2], x[1])))

#cogemos los generos mejor valorados por anyo y por mes
rddfinal = average.reduceByKey(lambda a,b: ((a[0], a[1]) if a[1] > b[1] else ((a[0]+ "; " + b[0], b[1]) if a[1] == b[1] else (b[0], b[1]))))

rddfinal.sortByKey(True)

rddfinal.coalesce(1,True).saveAsTextFile("solucionC.csv")