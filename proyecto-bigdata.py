
from pyspark.sql import SparkSession

from datetime import date

spark = SparkSession.builder \
        .master("local") \
        .appName("Proyecto Big Data") \
        .getOrCreate()

today = date.today()

day = today.strftime("%d-%m-%Y")

datos = spark.read.csv('s3://proyecto-bigdata/raw/datos-{0}.csv'.format(day), inferSchema=True, header=True)

datos_limpios = datos.drop('Código DIVIPOLA municipio','Código DIVIPOLA departamento','Unidad de medida de edad','Código ISO del país','Pertenencia étnica','Nombre del grupo étnico')

def edad_etapa(edad):
    etapa = ''
    if (edad<=5):
        etapa = 'primera infancia'
    elif (5<edad<=11):
        etapa = 'infancia'
    elif (11<edad<=18):
        etapa = 'adolescencia'
    elif (18<edad<=26):
        etapa = 'juventud'
    elif (26<edad<=59):
        etapa = 'adultez'
    elif (edad>=60):
        etapa = 'vejez'
    return etapa

#Agregar Columna
etapa_udf = spark.udf.register('Etapas', edad_etapa)

datos_limpios = datos_limpios.withColumn('Etapa', etapa_udf(datos['Edad']))

datos_limpios = datos_limpios.fillna('COLOMBIA', subset=['Nombre del país'])

datos_limpios.select('Nombre del país').show()

today = date.today()

d4 = today.strftime("%m-%d-%Y")

url = 's3://proyecto-bigdata/curated/{0}'.format(d4)

datos_limpios.coalesce(1).write.format('csv').option('header','True').save(url)

paises = datos_limpios.groupBy('Nombre del país').count().orderBy('count', ascending=False)

paises = paises.withColumnRenamed("Nombre del país","pais_donde_viajo")

recuperados_y_fallecidos_departamento = datos_limpios.groupBy('Nombre departamento','recuperado').count().orderBy('count', ascending=False)

recuperados_y_fallecidos_etapa = datos_limpios.groupBy('Etapa','recuperado').count().orderBy('count', ascending=False)

sexo = datos_limpios.groupBy('sexo').count().orderBy('count', ascending=False)

sexo_recuperado = datos_limpios.groupBy('sexo','recuperado').count().orderBy('count', ascending=False)

recuperados_muertos_medellin = datos_limpios.filter(datos_limpios['Nombre municipio'] == 'MEDELLIN').groupBy('Nombre municipio', 'recuperado').count().orderBy('count', ascending=False)

today = date.today()

d4 = today.strftime("%m-%d-%Y")

url_1 = 's3://proyecto-bigdata/refined/{0}/{1}'.format(d4, 'paises')
url_2 = 's3://proyecto-bigdata/refined/{0}/{1}'.format(d4, 'recuperados_y_fallecidos_departamento')
url_3 = 's3://proyecto-bigdata/refined/{0}/{1}'.format(d4, 'recuperados_y_fallecidos_etapa')
url_4 = 's3://proyecto-bigdata/refined/{0}/{1}'.format(d4, 'sexo')
url_5 = 's3://proyecto-bigdata/refined/{0}/{1}'.format(d4, 'recuperados_muertos_medellin')

paises.coalesce(1).write.format('csv').option('header','True').save(url_1)
recuperados_y_fallecidos_departamento.coalesce(1).write.format('csv').option('header','True').save(url_2)
recuperados_y_fallecidos_etapa.coalesce(1).write.format('csv').option('header','True').save(url_3)
sexo.coalesce(1).write.format('csv').option('header','True').save(url_4)
recuperados_muertos_medellin.coalesce(1).write.format('csv').option('header','True').save(url_5)
