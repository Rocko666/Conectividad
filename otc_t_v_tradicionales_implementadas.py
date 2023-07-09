# -- coding: utf-8 --
#########################################################################################################
# NOMBRE: otc_t_v_tradicionales_implementadas.py									                    #
# DESCRIPCION:																							#
#  Lee la tabla v_tradicionales_implementadas_DAS de la base externa SQL Server y la carga a Hive		#
# AUTOR: Karina Castro - Softconsulting	                            									#
# FECHA CREACION: 2022-09-19   																			#
#########################################################################################################
from pyspark import SparkContext, SparkConf, SQLContext
import pyodbc
import pandas as pd
import sys
import argparse
import datetime
import time
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql import HiveContext

parser = argparse.ArgumentParser()
parser.add_argument('--vUrl', required=True, type=str)
parser.add_argument('--vDatabase', required=True, type=str)
parser.add_argument('--vUid', required=True, type=str)
parser.add_argument('--bd', required=True, type=str)
parser.add_argument('--vPwd', required=True, type=str)
parser.add_argument('--vTabla', required=True, type=str)
parametros = parser.parse_args()
vUrl = parametros.vUrl
vDatabase = parametros.vDatabase
vUid = parametros.vUid
bd = parametros.bd
vPwd = parametros.vPwd
vTabla = parametros.vTabla

desde = time.time()

spark = SparkSession\
    .builder\
    .appName("OTC_T_V_TRADICIONALES_IMPLEMENTADAS")\
    .master("local")\
    .enableHiveSupport()\
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
sqlContext = SQLContext(sc)

query = "(SELECT item,codigo_caso,codbien,nomcli,ruccli,region,comercial,idc,jefecomercial,negocio,complejidad,tipo_requerimiento,numpuntos,canaldas, "
query = query + "servicio,medio,vser,vum,vequi,vnrc,vmrc,subsidio,fecha_entrada,nrc_actual,mrc_actual,caudal,codigo_das,codigo_localidad,nombre_das,"
query = query + "ruc_das,jeferegional,origen,fac_terminatx,numeroip,numeroventa,username,estado,canal_comercial"
query = query + " FROM "+vTabla +") as c"

df = sqlContext.read.format("jdbc") \
	.option("url",vUrl).option("dbtable", query) \
	.option("user", vUid).option("password", vPwd).option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

df = df.withColumn('nomcli', F.regexp_replace('nomcli', '\t', ''))

df.printSchema()

timestart_tbl = datetime.datetime.now()
print ("==== Guardando los datos en tabla "+bd+" ====")
df.repartition(10).write.mode("overwrite").insertInto(bd, overwrite=True)
df.printSchema()
timeend_tbl = datetime.datetime.now()
duracion_tbl = timeend_tbl - timestart_tbl
print("Escritura Exitosa de la tabla "+bd)
print("Duracion create "+bd+" {}".format(duracion_tbl))

spark.stop()
hasta = time.time()
duracion = hasta - desde
print("Duracion: {vDuracion}".format(vDuracion=duracion))