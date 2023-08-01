# encoding=utf8
#########################################################################################################
# NOMBRE: genera_archivo_txt.py        											                        #
# DESCRIPCION:																							#
#  Lee la tabla del extractor conectividad de hive y genera archivo en ruta output de BigData           #
# AUTOR: Karina Castro - Softconsulting	                            									#
# FECHA CREACION: 2022-09-19   																			#
#########################################################################################################
import sys
reload(sys)
sys.setdefaultencoding('windows-1252')
from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime
from pyspark.sql import functions as F, Window
import re
import argparse
from pyspark.sql.functions import *
# Genericos
sys.path.insert(1,'/var/opt/tel_spark')
from messages import *
from functions import *

parser = argparse.ArgumentParser()
parser.add_argument('--vRuta', required=True, type=str)
parser.add_argument('--vTabla', required=True, type=str)
parser.add_argument('--vFecha_Ini', required=True, type=str)
parser.add_argument('--vFecha_Fin', required=True, type=str)
parametros = parser.parse_args()
vRuta=parametros.vRuta
vTabla=parametros.vTabla
vFecha_Ini=parametros.vFecha_Ini
vFecha_Fin=parametros.vFecha_Fin

timestartmain = datetime.now() 

spark = SparkSession\
	.builder\
	.appName("EXTRACTOR_CONECTIVIDAD")\
    .config("spark.sql.broadcastTimeout", "36000") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("hive.enforce.bucketing", "false")\
	.config("hive.enforce.sorting", "false")\
	.enableHiveSupport()\
	.getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

vFecha_Ini_Trn=datetime.strptime(vFecha_Ini, '%Y-%m-%d %H:%M:%S')
vFecha_Fin_Trn=datetime.strptime(vFecha_Fin, '%Y-%m-%d %H:%M:%S')
print("Transform 1: "+ str(vFecha_Ini_Trn))
print("Type: "+ str(type(vFecha_Ini_Trn)))
print("Transform 2: "+ str(vFecha_Fin_Trn))
print("Type: "+ str(type(vFecha_Fin_Trn)))

vSql = """
SELECT tipo,
id_producto,
id_movimiento,
fecha_proceso,
fecha_implementacion,
codigo_iasi,
codigo_iasi_conectividad,
codigo_bien,
id_acceso,
numero_venta,
cliente,
ruc_cliente,
cuenta,
servicio,
producto,
slo,
tipo_requerimiento,
ancho_de_banda,
medio,
suma_de_vser,
suma_de_vum,
suma_de_vequi,
mrc,
mrc_anterior,
nrc,
nrc_anterior,
subsidio,
complejidad,
idc,
region,
jefe_regional,
jefe_comercial,
codigo_das,
localidad,
id_canal,
canaldas,
ruc_das,
nombre_das,
codigo_usuario,
comercial_das,
fecha_desinstalacion,
canal_comercial,
codigoref,
estado,
id_subcanal,
subcanal,
delta_renta,
UPPER(MD5(id_hash)) AS id_hash
FROM (SELECT tipo,
(CASE WHEN id_producto IS NULL THEN 'N/A' ELSE id_producto END) AS id_producto,
(CASE WHEN id_movimiento IS NULL THEN 'N/A' ELSE id_movimiento END) AS id_movimiento,
fecha_proceso,
(CASE WHEN fecha_implementacion IS NULL THEN '01/01/1990' 
ELSE concat_ws('/',SUBSTR(date_format(fecha_implementacion,'yyyy-MM-dd'),9,2),SUBSTR(date_format(fecha_implementacion,'yyyy-MM-dd'),6,2),SUBSTR(date_format(fecha_implementacion,'yyyy-MM-dd'),1,4)) END) AS fecha_implementacion,
codigo_iasi,
codigo_iasi_conectividad,
codigo_bien,
id_acceso,
numero_venta,
cliente,
ruc AS ruc_cliente,
cuenta,
servicio,
producto,
slo,
tipo_requerimiento,
ancho_de_banda,
medio,
suma_de_vser,
suma_de_vum,
suma_de_vequi,
mrc,
mrc_anterior,
nrc,
nrc_anterior,
subsidio,
complejidad,
idc,
region,
jefe_regional,
jefe_comercial,
codigo_das,
localidad,
(CASE WHEN id_canal IS NULL THEN 'N/A' ELSE id_canal END) AS id_canal,
canal AS canaldas,
ruc_das,
nombre_das,
codigo_usuario,
comercial_das,
(CASE WHEN (fecha_desinstalacion IS NULL OR fecha_desinstalacion='') THEN '01/01/1990' 
ELSE concat_ws('/',SUBSTR(date_format(fecha_desinstalacion,'yyyy-MM-dd'),9,2),SUBSTR(date_format(fecha_desinstalacion,'yyyy-MM-dd'),6,2),SUBSTR(date_format(fecha_desinstalacion,'yyyy-MM-dd'),1,4)) END) AS fecha_desinstalacion,
canal_comercial,
codigoref,
estado,
(CASE WHEN id_subcanal IS NULL THEN 'N/A' ELSE id_subcanal END) AS id_subcanal,
subcanal,
delta_renta,
id_hash
FROM {vTabla}
WHERE from_unixtime(unix_timestamp(fecha_implementacion,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss')>='{vFecha_Ini_Trn}'
AND from_unixtime(unix_timestamp(fecha_implementacion,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss')<'{vFecha_Fin_Trn}'
AND tipo<>'BAJA'

UNION ALL

SELECT tipo,
(CASE WHEN id_producto IS NULL THEN 'N/A' ELSE id_producto END) AS id_producto,
(CASE WHEN id_movimiento IS NULL THEN 'N/A' ELSE id_movimiento END) AS id_movimiento,
fecha_proceso,
(CASE WHEN fecha_implementacion IS NULL THEN '01/01/1990' 
ELSE concat_ws('/',SUBSTR(date_format(fecha_implementacion,'yyyy-MM-dd'),9,2),SUBSTR(date_format(fecha_implementacion,'yyyy-MM-dd'),6,2),SUBSTR(date_format(fecha_implementacion,'yyyy-MM-dd'),1,4)) END) AS fecha_implementacion,
codigo_iasi,
codigo_iasi_conectividad,
codigo_bien,
id_acceso,
numero_venta,
cliente,
ruc AS ruc_cliente,
cuenta,
servicio,
producto,
slo,
tipo_requerimiento,
ancho_de_banda,
medio,
suma_de_vser,
suma_de_vum,
suma_de_vequi,
mrc,
mrc_anterior,
nrc,
nrc_anterior,
subsidio,
complejidad,
idc,
region,
jefe_regional,
jefe_comercial,
codigo_das,
localidad,
(CASE WHEN id_canal IS NULL THEN 'N/A' ELSE id_canal END) AS id_canal,
canal AS canaldas,
ruc_das,
nombre_das,
codigo_usuario,
comercial_das,
(CASE WHEN (fecha_desinstalacion IS NULL OR fecha_desinstalacion='') THEN '01/01/1990' 
ELSE concat_ws('/',SUBSTR(date_format(fecha_desinstalacion,'yyyy-MM-dd'),9,2),SUBSTR(date_format(fecha_desinstalacion,'yyyy-MM-dd'),6,2),SUBSTR(date_format(fecha_desinstalacion,'yyyy-MM-dd'),1,4)) END) AS fecha_desinstalacion,
canal_comercial,
codigoref,
estado,
(CASE WHEN id_subcanal IS NULL THEN 'N/A' ELSE id_subcanal END) AS id_subcanal,
subcanal,
delta_renta,
id_hash
FROM {vTabla}
WHERE from_unixtime(unix_timestamp(fecha_desinstalacion,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss')>='{vFecha_Ini_Trn}'
AND from_unixtime(unix_timestamp(fecha_desinstalacion,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss')<'{vFecha_Fin_Trn}'
AND tipo='BAJA') a

"""

timestart = datetime.now()
print ("==== Generando archivo "+vRuta+" ====")
df_final=spark.sql(vSql.format(vTabla=vTabla,vFecha_Ini_Trn=vFecha_Ini_Trn,vFecha_Fin_Trn=vFecha_Fin_Trn))
print(etq_sql(vSql.format(vTabla=vTabla,vFecha_Ini_Trn=vFecha_Ini_Trn,vFecha_Fin_Trn=vFecha_Fin_Trn)))
print('Conteo:', df_final.count())
pandas_df = df_final.toPandas()
pandas_df.rename(columns = lambda x:x.upper(), inplace=True )
pandas_df.to_csv(vRuta, sep='|',index=False)
timeend = datetime.now()
duracion = timeend - timestart
print("Generacion exitosa del archivo "+vRuta)
print("Duracion genera archivo "+vRuta+" {}".format(duracion))

spark.stop()
timeendmain = datetime.now()
duracion = timestartmain- timeendmain 
print("Duracion: {vDuracion}".format(vDuracion=duracion))
