from pyspark import SparkConf, SparkContext
from pyspark import SQLContext
import string


conf = SparkConf().setAppName('Project')
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

meses = ['Enero','Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']
estaciones = ['Primavera', 'Verano', 'Otonyo', 'Invierno']
epocas = ['Navidad', 'Vacaciones', 'Periodo Lectivo']

#leer los archivos con meses, estaciones y epocas
dfMeses = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferschema", "true").load("meses.csv/part-00000*.csv")
dfEstaciones = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferschema", "true").load("estaciones.csv/part-00000*.csv")
dfEpocas = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferschema", "true").load("epocas.csv/part-00000*.csv")

solMeses = dfMeses.rdd
solEstaciones = dfEstaciones.rdd
solEpocas = dfEpocas.rdd

#tenemos solMeses = (MES, GENERO), NUMVECES
solMeses = solMeses.map(lambda x: ((x[1], x[0]), x[2]))
solMeses = solMeses.reduceByKey(lambda a, b: a + b)

#solEstaciones = (ESTACION, GENERO), NUMERO
solEstaciones = solEstaciones.map(lambda x: ((x[1], x[0]), x[2]))
solEstaciones = solEstaciones.reduceByKey(lambda a, b: a + b)

#solEpocas = (EPOCA, GENERO), NUMERO
solEpocas = solEpocas.map(lambda x: ((x[1], x[0]), x[2]))
solEpocas = solEpocas.reduceByKey(lambda a, b: a + b)

maximo = sc.emptyRDD() #en maximo guardaremos los top 5 generos por mes, epoca y estacion
minimo = sc.emptyRDD() #en minimo guardaremos el peor genero por mes, epoca y estacion

for mes in meses:

	rdd4 = solMeses.filter(lambda x: mes == x[0][0]) #cogemos las filas que tengan el mes que queremos
	rdd4 = rdd4.map(lambda x: (x[1], x[0][1]))
	rdd4 = rdd4.sortByKey(False) #ordenamos de mayor a menor numero de visualizaciones
	rdd5 = rdd4.sortByKey(True) #ordenamos de menor a mayor numero de visualizaciones
	
	if rdd4.count() > 0:
		rdd4 = rdd4.take(5) #cogemos los 5 mayores
		rdd5 = rdd5.take(1) #y el menor
		rdd4 = sc.parallelize(rdd4) #los pasamos a rdd
		rdd5 = sc.parallelize(rdd5)
		rdd4 = rdd4.map(lambda x: (mes, x[1], x[0]))
		rdd5 = rdd5.map(lambda x: (mes, x[1], x[0]))
		maximo = maximo.union(rdd4)
		minimo = minimo.union(rdd5)

for estacion in estaciones:

	rdd4 = solEstaciones.filter(lambda x: estacion == x[0][0])
	rdd4 = rdd4.map(lambda x: (x[1], x[0][1]))
	rdd4 = rdd4.sortByKey(False)
	rdd5 = rdd4.sortByKey(True)
	
	if rdd4.count() > 0:
		rdd4 = rdd4.take(5)
		rdd5 = rdd5.take(1)
		rdd4 = sc.parallelize(rdd4)
		rdd5 = sc.parallelize(rdd5)
		rdd4 = rdd4.map(lambda x: (estacion, x[1], x[0]))
		rdd5 = rdd5.map(lambda x: (estacion, x[1], x[0]))
		maximo = maximo.union(rdd4)
		minimo = minimo.union(rdd5)

for periodo in epocas:

	rdd4 = solEpocas.filter(lambda x: periodo == x[0][0])
	rdd4 = rdd4.map(lambda x: (x[1], x[0][1]))
	rdd4 = rdd4.sortByKey(False)
	rdd5 = rdd4.sortByKey(True)
	
	if rdd4.count() > 0:
		rdd4 = rdd4.take(5)
		rdd5 = rdd5.take(1)
		rdd4 = sc.parallelize(rdd4)
		rdd5 = sc.parallelize(rdd5)
		rdd4 = rdd4.map(lambda x: (periodo, x[1], x[0]))
		rdd5 = rdd5.map(lambda x: (periodo, x[1], x[0]))
		maximo = maximo.union(rdd4)
		minimo = minimo.union(rdd5)

maximo.coalesce(1,True).saveAsTextFile("maximos.txt")
minimo.coalesce(1,True).saveAsTextFile("minimos.txt")

