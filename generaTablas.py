from pyspark import SparkConf, SparkContext
from pyspark import SQLContext
import string
import datetime

conf = SparkConf().setAppName('Project')
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferschema", "true").load("ratings.csv")
df2 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferschema", "true").load("movies.csv")

dfFinal = df2.join(df, ['movieId'], "outer")

rdd = dfFinal.rdd

rdd = rdd.filter(lambda x: x[5] is not None) #comprobamos que tenga fecha

#tenemos el titulo-generos-rating-fecha
rdd = rdd.map(lambda x: (x[1], x[2], x[4], datetime.datetime.fromtimestamp(x[5]).strftime('%Y-%m-%d %H:%M:%S')))

genres = ['Action','Adventure','Animation',"Children's",'Comedy','Crime','Documentary','Drama','Fantasy','Film-Noir','Horror','Musical','Mystery','Romance','Sci-Fi','Thriller','War','Western']
meses = ['Enero','Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']
estaciones = ['Primavera', 'Verano', 'Otonyo', 'Invierno']
epocas = ['Navidad', 'Vacaciones', 'Periodo Lectivo']

rdd = rdd.map(lambda x: (x[0], x[1], x[2], meses[int(str(x[3]).split('-')[1]) - 1],
													 'Primavera' if (int(str(x[3]).split(' ')[0].split('-')[2]) > 20 and int(str(x[3]).split(' ')[0].split('-')[1]) == 3) 
														 or (int(str(x[3]).split(' ')[0].split('-')[1]) == 6 and int(str(x[3]).split(' ')[0].split('-')[2]) < 21)
														 or (int(str(x[3]).split(' ')[0].split('-')[1]) == 4) or (int(str(x[3]).split(' ')[0].split('-')[1]) == 5)
													
													else (

														'Verano' if (int(str(x[3]).split(' ')[0].split('-')[2]) > 20 and int(str(x[3]).split(' ')[0].split('-')[1]) == 6) 
														 or (int(str(x[3]).split(' ')[0].split('-')[1]) == 9 and int(str(x[3]).split(' ')[0].split('-')[2]) < 21)
														 or (int(str(x[3]).split(' ')[0].split('-')[1]) == 7) or (int(str(x[3]).split(' ')[0].split('-')[1]) == 8)

													else (

														'Otonyo' if (int(str(x[3]).split(' ')[0].split('-')[2]) > 20 and int(str(x[3]).split(' ')[0].split('-')[1]) == 9) 
														 or (int(str(x[3]).split(' ')[0].split('-')[1]) == 12 and int(str(x[3]).split(' ')[0].split('-')[2]) < 21)
														 or (int(str(x[3]).split(' ')[0].split('-')[1]) == 10) or (int(str(x[3]).split(' ')[0].split('-')[1]) == 11)

													else 'Invierno'

													)), 
													'Navidad' if (int(str(x[3]).split(' ')[0].split('-')[2]) > 21 and int(str(x[3]).split(' ')[0].split('-')[1]) == 12) 
														 or (int(str(x[3]).split(' ')[0].split('-')[1]) == 1 and int(str(x[3]).split(' ')[0].split('-')[2]) < 9)

													else ('Vacaciones' if (int(str(x[3]).split(' ')[0].split('-')[2]) > 20 and int(str(x[3]).split(' ')[0].split('-')[1]) == 6) 
														 or (int(str(x[3]).split(' ')[0].split('-')[1]) == 9 and int(str(x[3]).split(' ')[0].split('-')[2]) < 21)
														 or (int(str(x[3]).split(' ')[0].split('-')[1]) == 7) or (int(str(x[3]).split(' ')[0].split('-')[1]) == 8)

													else 'Periodo Lectivo'

													))) #rdd con TITULO-GENERO-RATING-ESTACION



rddMeses = rdd.map(lambda x: ((x[3], x[1]), 1)) #key-pair con (MES,GENERO)como clave y un 1 como valor
rddMeses2 = rdd.map(lambda x: ((x[3], x[1]), x[2]))
solMeses = rddMeses.reduceByKey(lambda a, b: a + b)
solMeses2 = rddMeses2.reduceByKey(lambda a, b: a + b)


solMeses = solMeses.map(lambda x: (x[0][0], x[0][1], x[1]))

rdd1 = sc.emptyRDD()

for genero in genres:
	rdd2 = solMeses.filter(lambda x: genero in x[1])
	rdd2 = rdd2.map(lambda x: (genero, x[0], x[2]))
	rdd1 = rdd1.union(rdd2)

rdd1.toDF().coalesce(1).write.format("com.databricks.spark.csv").save("meses.csv") #este sera nuestro rdd base

rddEstaciones = rdd.map(lambda x: ((x[4], x[1]), 1)) #key-pair con (ESTACION,GENERO)como clave y un 1 como valor
solEstaciones = rddEstaciones.reduceByKey(lambda a, b: a + b)
solEstaciones = solEstaciones.map(lambda x: (x[0][0], x[0][1], x[1]))

rdd1 = sc.emptyRDD()
for genero in genres:
	rdd2 = solEstaciones.filter(lambda x: genero in x[1])
	rdd2 = rdd2.map(lambda x: (genero, x[0], x[2]))
	rdd1 = rdd1.union(rdd2)

rdd1.toDF().coalesce(1).write.format("com.databricks.spark.csv").save("estaciones.csv") #este sera nuestro rdd base

rddEpocas = rdd.map(lambda x: ((x[5], x[1]), 1)) #key-pair con (EPOCA,GENERO)como clave y un 1 como valor
solEpocas = rddEpocas.reduceByKey(lambda a, b: a + b)
solEpocas = solEpocas.map(lambda x: (x[0][0], x[0][1], x[1]))

rdd1 = sc.emptyRDD()
for genero in genres:
	rdd2 = solEpocas.filter(lambda x: genero in x[1])
	rdd2 = rdd2.map(lambda x: (genero, x[0], x[2]))
	rdd1 = rdd1.union(rdd2)

rdd1.toDF().coalesce(1).write.format("com.databricks.spark.csv").save("epocas.csv") #este sera nuestro rdd base
