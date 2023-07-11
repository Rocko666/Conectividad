# -*- coding: utf-8 -*-
#########################################################################################################
# NOMBRE: otc_t_ext_conectividad.py												                        #
# DESCRIPCION:																							#
#  Lee tabla RAW, realiza transformaciones, une la informacion y carga a tabla en Hive					#
# AUTOR: Karina Castro - Softconsulting	                            									#
# FECHA CREACION: 2022-09-19   																			#
#########################################################################################################
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import datetime
from pyspark.sql import functions as F, Window
from pyspark.sql import SparkSession
from pyspark import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.types import StructType, DoubleType, DateType, StringType, FloatType, TimestampType, StructField, IntegerType, BooleanType
import argparse

timestart = datetime.datetime.now() 
parser = argparse.ArgumentParser()
parser.add_argument('--vFecha_Proceso', required=True, type=str)
parser.add_argument('--bd', required=True, type=str)
parametros = parser.parse_args()
vFecha_Proceso = parametros.vFecha_Proceso
bd = parametros.bd

spark = SparkSession\
    .builder\
    .appName("EXTRACTOR_CONECTIVIDAD")\
    .config("spark.sql.broadcastTimeout", "36000") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.yarn.queue", "reportes") \
    .config("hive.enforce.bucketing", "false")\
    .config("hive.enforce.sorting", "false")\
    .master("yarn")\
    .enableHiveSupport()\
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

spark.conf.set("spark.sql.crossJoin.enabled", "true")

vTexto01="DesinstalaciÃ³n"
vTexto=vTexto01.decode("utf-8")

vSql="""

SELECT a.tipo,
a.fecha_proceso,
a.fecha_implementacion,
a.codigo_iasi,
a.codigo_bien,
a.id_acceso,
a.numero_venta,
a.cliente,
a.ruc,
a.cuenta,
a.servicio,
a.producto,
a.slo,
a.tipo_requerimiento,
a.ancho_de_banda,
a.medio,
a.suma_de_vser,
a.suma_de_vum,
a.suma_de_vequi,
a.mrc,
a.mrc_anterior,
a.nrc,
a.nrc_anterior,
a.subsidio,
a.complejidad,
a.idc,
a.region,
a.jefe_regional,
a.jefe_comercial,
a.codigo_das,
a.localidad,
a.canal,
a.ruc_das,
a.codigo_usuario,
a.comercial_das,
a.fecha_desinstalacion,
a.id_producto,
UPPER(MD5(CONCAT(CONCAT(a.codigo_iasi,a.fecha_proceso), row_number() over(order by a.fecha_proceso)))) AS id_hash,
a.id_movimiento,
a.id_canal,
a.codigo_iasi_conectividad,
a.nombre_das,
a.canal_comercial,
a.codigoref,
a.estado,
a.id_subcanal,
a.subcanal,
a.delta_renta
FROM (SELECT 'DIGITAL IMPLEMENTADO' AS tipo,
'{vFecha_Proceso}' AS fecha_proceso,
a.fecha_ganada AS fecha_implementacion,
a.item AS codigo_iasi,
a.codbien AS codigo_bien,
a.numven AS id_acceso,
CAST('' AS string) AS numero_venta,
a.nomcli AS cliente,
a.ruccli AS ruc,
CAST(a.cuenta AS string) AS cuenta,
a.servicio AS servicio,
a.negocio AS producto,
a.slo AS slo,
a.tipo_requerimiento AS tipo_requerimiento,
NULL AS ancho_de_banda,
CAST('' AS string) AS medio,
NULL AS suma_de_vser,
NULL AS suma_de_vum,
NULL AS suma_de_vequi,
a.vmrc AS mrc,
NULL AS mrc_anterior,
a.vnrc AS nrc,
NULL AS nrc_anterior,
NULL AS subsidio,
a.complejidad AS complejidad,
a.idc AS idc,
a.region AS region,
a.jeferegional AS jefe_regional,
a.jefecomercial AS jefe_comercial,
a.codigo_das AS codigo_das,
a.codigo_localidad AS localidad,
a.canaldas AS canal,
a.ruc_das,
a.username AS codigo_usuario,
a.comercial AS comercial_das,
NULL AS fecha_desinstalacion,
b.id_tipo_movimiento AS id_producto,
c.id_tipo_movimiento AS id_movimiento,
d.id_tipo_movimiento AS id_canal,
CAST('' AS string) AS codigo_iasi_conectividad,
a.nombre_das,
a.canal_comercial,
a.codigoref,
a.estado,
e.id_tipo_movimiento AS id_subcanal,
(CASE WHEN a.canal_comercial='NEGOCIOS INDIRECTOS' THEN 'DISTRIBUIDOR PYMES' ELSE 'CC COMERCIAL CALLCENTER B2B' END) AS subcanal,
0 AS delta_renta
FROM db_userdas.otc_t_v_digitales_implementadas a
LEFT JOIN (SELECT DISTINCT tipo_movimiento,id_tipo_movimiento 
FROM db_reportes.otc_t_catalogo_consolidado_id
WHERE extractor='Conectividad'
AND nombre_id='ID_PRODUCTO') b
ON UPPER(a.negocio)=UPPER(b.tipo_movimiento)
LEFT JOIN (SELECT DISTINCT tipo_movimiento,id_tipo_movimiento 
FROM db_reportes.otc_t_catalogo_consolidado_id
WHERE extractor='Conectividad'
AND nombre_id='ID_TIPO_MOVIMIENTO') c
ON UPPER(a.tipo_requerimiento)=UPPER(c.tipo_movimiento)
LEFT JOIN (SELECT DISTINCT tipo_movimiento,id_tipo_movimiento 
FROM db_reportes.otc_t_catalogo_consolidado_id
WHERE extractor='Todos'
AND nombre_id='ID_CANAL') d
ON UPPER(a.canal_comercial)=UPPER(d.tipo_movimiento)
LEFT JOIN (SELECT DISTINCT tipo_movimiento,id_tipo_movimiento 
FROM db_reportes.otc_t_catalogo_consolidado_id
WHERE extractor='Todos'
AND nombre_id='ID_SUBCANAL') e
ON UPPER(CASE WHEN a.canal_comercial='NEGOCIOS INDIRECTOS' THEN 'DISTRIBUIDOR PYMES' ELSE 'CC COMERCIAL CALLCENTER B2B' END)=UPPER(e.tipo_movimiento)

UNION ALL

SELECT 'DIGITAL GANADA' AS tipo,
'{vFecha_Proceso}' AS fecha_proceso,
a.fecha_ganada AS fecha_implementacion,
a.item AS codigo_iasi,
a.codbien AS codigo_bien,
a.numven AS id_acceso,
CAST('' AS string) AS numero_venta,
a.nomcli AS cliente,
a.ruccli AS ruc,
CAST(a.cuenta AS string) AS cuenta,
a.servicio AS servicio,
a.negocio AS producto,
a.slo AS slo,
a.tipo_requerimiento AS tipo_requerimiento,
NULL AS ancho_de_banda,
CAST('' AS string) AS medio,
NULL AS suma_de_vser,
NULL AS suma_de_vum,
NULL AS suma_de_vequi,
a.vmrc AS mrc,
NULL AS mrc_anterior,
a.vnrc AS nrc,
NULL AS nrc_anterior,
NULL AS subsidio,
a.complejidad AS complejidad,
a.idc AS idc,
a.region AS region,
a.jeferegional AS jefe_regional,
a.jefecomercial AS jefe_comercial,
a.codigo_das AS codigo_das,
a.codigo_localidad AS localidad,
a.canaldas AS canal,
a.ruc_das,
a.username AS codigo_usuario,
a.comercial AS comercial_das,
NULL AS fecha_desinstalacion,
b.id_tipo_movimiento AS id_producto,
c.id_tipo_movimiento AS id_movimiento,
d.id_tipo_movimiento AS id_canal,
CAST('' AS string) AS codigo_iasi_conectividad,
a.nombre_das,
a.canal_comercial,
CAST('' AS string) AS codigoref,
CAST('' AS string) AS estado,
e.id_tipo_movimiento AS id_subcanal,
(CASE WHEN a.canal_comercial='NEGOCIOS INDIRECTOS' THEN 'DISTRIBUIDOR PYMES' ELSE 'CC COMERCIAL CALLCENTER B2B' END) AS subcanal,
0 AS delta_renta
FROM db_userdas.otc_t_v_digitales_ganadas a
LEFT JOIN (SELECT DISTINCT tipo_movimiento,id_tipo_movimiento 
FROM db_reportes.otc_t_catalogo_consolidado_id
WHERE extractor='Conectividad'
AND nombre_id='ID_PRODUCTO') b
ON UPPER(a.negocio)=UPPER(b.tipo_movimiento)
LEFT JOIN (SELECT DISTINCT tipo_movimiento,id_tipo_movimiento 
FROM db_reportes.otc_t_catalogo_consolidado_id
WHERE extractor='Conectividad'
AND nombre_id='ID_TIPO_MOVIMIENTO') c
ON UPPER(a.tipo_requerimiento)=UPPER(c.tipo_movimiento)
LEFT JOIN (SELECT DISTINCT tipo_movimiento,id_tipo_movimiento 
FROM db_reportes.otc_t_catalogo_consolidado_id
WHERE extractor='Todos'
AND nombre_id='ID_CANAL') d
ON UPPER(a.canal_comercial)=UPPER(d.tipo_movimiento)
LEFT JOIN (SELECT DISTINCT tipo_movimiento,id_tipo_movimiento 
FROM db_reportes.otc_t_catalogo_consolidado_id
WHERE extractor='Todos'
AND nombre_id='ID_SUBCANAL') e
ON UPPER(CASE WHEN a.canal_comercial='NEGOCIOS INDIRECTOS' THEN 'DISTRIBUIDOR PYMES' ELSE 'CC COMERCIAL CALLCENTER B2B' END)=UPPER(e.tipo_movimiento)

UNION ALL

SELECT 'CONECTIVIDAD IMPLEMENTADA' AS tipo,
'{vFecha_Proceso}' AS fecha_proceso,
a.fecha_entrada AS fecha_implementacion,
a.item AS codigo_iasi,
a.codbien AS codigo_bien,
a.numeroip AS id_acceso,
a.numeroventa AS numero_venta,
a.nomcli AS cliente,
a.ruccli AS ruc,
a.fac_terminatx AS cuenta,
CAST('' AS string) AS servicio,
a.servicio AS producto,
CAST('' AS string) AS slo,
a.tipo_requerimiento AS tipo_requerimiento,
a.caudal AS ancho_de_banda,
a.medio AS medio,
a.vser AS suma_de_vser,
a.vum AS suma_de_vum,
a.vequi AS suma_de_vequi,
a.vmrc AS mrc,
a.mrc_actual AS mrc_anterior,
a.vnrc AS nrc,
a.nrc_actual AS nrc_anterior,
a.subsidio AS subsidio,
a.complejidad AS complejidad,
a.idc,
a.region,
a.jeferegional AS jefe_regional,
a.jefecomercial AS jefe_comercial,
a.codigo_das,
a.codigo_localidad AS localidad,
a.canaldas AS canal,
a.ruc_das,
a.username AS codigo_usuario,
a.comercial AS comercial_das,
NULL AS fecha_desinstalacion,
b.id_tipo_movimiento AS id_producto,
c.id_tipo_movimiento AS id_movimiento,
d.id_tipo_movimiento AS id_canal,
a.codigo_caso AS codigo_iasi_conectividad,
a.nombre_das,
a.canal_comercial,
CAST('' AS string) AS codigoref,
a.estado,
e.id_tipo_movimiento AS id_subcanal,
(CASE WHEN a.canal_comercial='NEGOCIOS INDIRECTOS' THEN 'DISTRIBUIDOR PYMES' ELSE 'CC COMERCIAL CALLCENTER B2B' END) AS subcanal,
(a.vmrc - a.mrc_actual) AS delta_renta
FROM db_userdas.otc_t_v_tradicionales_implementadas a
LEFT JOIN (SELECT DISTINCT tipo_movimiento,id_tipo_movimiento 
FROM db_reportes.otc_t_catalogo_consolidado_id
WHERE extractor='Conectividad'
AND nombre_id='ID_PRODUCTO') b
ON UPPER(a.servicio)=UPPER(b.tipo_movimiento)
LEFT JOIN (SELECT DISTINCT tipo_movimiento,id_tipo_movimiento 
FROM db_reportes.otc_t_catalogo_consolidado_id
WHERE extractor='Conectividad'
AND nombre_id='ID_TIPO_MOVIMIENTO') c
ON UPPER(a.tipo_requerimiento)=UPPER(c.tipo_movimiento)
LEFT JOIN (SELECT DISTINCT tipo_movimiento,id_tipo_movimiento 
FROM db_reportes.otc_t_catalogo_consolidado_id
WHERE extractor='Todos'
AND nombre_id='ID_CANAL') d
ON UPPER(a.canal_comercial)=UPPER(d.tipo_movimiento)
LEFT JOIN (SELECT DISTINCT tipo_movimiento,id_tipo_movimiento 
FROM db_reportes.otc_t_catalogo_consolidado_id
WHERE extractor='Todos'
AND nombre_id='ID_SUBCANAL') e
ON UPPER(CASE WHEN a.canal_comercial='NEGOCIOS INDIRECTOS' THEN 'DISTRIBUIDOR PYMES' ELSE 'CC COMERCIAL CALLCENTER B2B' END)=UPPER(e.tipo_movimiento)

UNION ALL

SELECT 'CONECTIVIDAD GANADA' AS tipo,
'{vFecha_Proceso}' AS fecha_proceso,
a.fecha_ganada AS fecha_implementacion,
a.item AS codigo_iasi,
a.codbien AS codigo_bien,
a.numeroip AS id_acceso,
a.numeroventa AS numero_venta,
a.nomcli AS cliente,
a.ruccli AS ruc,
a.fac_terminatx AS cuenta,
CAST('' AS string) AS servicio,
a.servicio AS producto,
CAST('' AS string) AS slo,
a.tipo_requerimiento AS tipo_requerimiento,
a.caudal AS ancho_de_banda,
a.medio AS medio,
a.vser AS suma_de_vser,
a.vum AS suma_de_vum,
a.vequi AS suma_de_vequi,
a.vmrc AS mrc,
a.mrc_actual AS mrc_anterior,
a.vnrc AS nrc,
a.nrc_actual AS nrc_anterior,
a.subsidio AS subsidio,
a.complejidad AS complejidad,
a.idc,
a.region,
a.jeferegional AS jefe_regional,
a.jefecomercial AS jefe_comercial,
a.codigo_das,
a.codigo_localidad AS localidad,
a.canaldas AS canal,
a.ruc_das,
a.username AS codigo_usuario,
a.comercial AS comercial_das,
NULL AS fecha_desinstalacion,
b.id_tipo_movimiento AS id_producto,
c.id_tipo_movimiento AS id_movimiento,
d.id_tipo_movimiento AS id_canal,
codigo_caso AS codigo_iasi_conectividad,
nombre_das,
a.canal_comercial,
CAST('' AS string) AS codigoref,
CAST('' AS string) AS estado,
e.id_tipo_movimiento AS id_subcanal,
(CASE WHEN a.canal_comercial='NEGOCIOS INDIRECTOS' THEN 'DISTRIBUIDOR PYMES' ELSE 'CC COMERCIAL CALLCENTER B2B' END) AS subcanal,
(a.vmrc - a.mrc_actual) AS delta_renta
FROM db_userdas.otc_t_v_tradicionales_ganadas a
LEFT JOIN (SELECT DISTINCT tipo_movimiento,id_tipo_movimiento 
FROM db_reportes.otc_t_catalogo_consolidado_id
WHERE extractor='Conectividad'
AND nombre_id='ID_PRODUCTO') b
ON UPPER(a.servicio)=UPPER(b.tipo_movimiento)
LEFT JOIN (SELECT DISTINCT tipo_movimiento,id_tipo_movimiento 
FROM db_reportes.otc_t_catalogo_consolidado_id
WHERE extractor='Conectividad'
AND nombre_id='ID_TIPO_MOVIMIENTO') c
ON UPPER(a.tipo_requerimiento)=UPPER(c.tipo_movimiento)
LEFT JOIN (SELECT DISTINCT tipo_movimiento,id_tipo_movimiento 
FROM db_reportes.otc_t_catalogo_consolidado_id
WHERE extractor='Todos'
AND nombre_id='ID_CANAL') d
ON UPPER(a.canal_comercial)=UPPER(d.tipo_movimiento)
LEFT JOIN (SELECT DISTINCT tipo_movimiento,id_tipo_movimiento 
FROM db_reportes.otc_t_catalogo_consolidado_id
WHERE extractor='Todos'
AND nombre_id='ID_SUBCANAL') e
ON UPPER(CASE WHEN a.canal_comercial='NEGOCIOS INDIRECTOS' THEN 'DISTRIBUIDOR PYMES' ELSE 'CC COMERCIAL CALLCENTER B2B' END)=UPPER(e.tipo_movimiento)

UNION ALL

SELECT 'BAJA' AS tipo,
'{vFecha_Proceso}' AS fecha_proceso,
a.fecha_implementacion AS fecha_implementacion,
a.item AS codigo_iasi,
a.codbien AS codigo_bien,
a.idAcceso AS id_acceso,
a.numeroventa AS numero_venta,
a.nomcli AS cliente,
a.ruccli AS ruc,
CAST('' AS string) AS cuenta,
CAST('' AS string) AS servicio,
a.servicio AS producto,
CAST('' AS string) AS slo,
'{vTexto}' AS tipo_requerimiento,
a.caudal AS ancho_de_banda,
a.medio AS medio,
a.vser AS suma_de_vser,
a.vum AS suma_de_vum,
a.vequi AS suma_de_vequi,
a.vmrc AS mrc,
NULL AS mrc_anterior,
a.vnrc AS nrc,
NULL AS nrc_anterior,
NULL AS subsidio,
a.complejidad AS complejidad,
a.idc,
a.region,
a.jeferegional AS jefe_regional,
a.jefecomercial AS jefe_comercial,
a.codigo_das,
a.codigo_localidad AS localidad,
a.canaldas AS canal,
a.ruc_das,
CAST('' AS string) AS codigo_usuario,
a.comercial AS comercial_das,
a.fecha_salida AS fecha_desinstalacion,
b.id_tipo_movimiento AS id_producto,
c.id_tipo_movimiento AS id_movimiento,
d.id_tipo_movimiento AS id_canal,
a.codigo_caso AS codigo_iasi_conectividad,
a.nombre_das,
a.canal_comercial,
CAST('' AS string) AS codigoref,
CAST('' AS string) AS estado,
e.id_tipo_movimiento AS id_subcanal,
(CASE WHEN a.canal_comercial='NEGOCIOS INDIRECTOS' THEN 'DISTRIBUIDOR PYMES' ELSE 'CC COMERCIAL CALLCENTER B2B' END) AS subcanal,
0 AS delta_renta
FROM db_userdas.otc_t_v_bajas_servicios a
LEFT JOIN (SELECT DISTINCT tipo_movimiento,id_tipo_movimiento 
FROM db_reportes.otc_t_catalogo_consolidado_id
WHERE extractor='Conectividad'
AND nombre_id='ID_PRODUCTO') b
ON UPPER(a.servicio)=UPPER(b.tipo_movimiento)
LEFT JOIN (SELECT DISTINCT tipo_movimiento,id_tipo_movimiento 
FROM db_reportes.otc_t_catalogo_consolidado_id
WHERE extractor='Conectividad'
AND nombre_id='ID_TIPO_MOVIMIENTO') c
ON UPPER('{vTexto}')=UPPER(c.tipo_movimiento)
LEFT JOIN (SELECT DISTINCT tipo_movimiento,id_tipo_movimiento 
FROM db_reportes.otc_t_catalogo_consolidado_id
WHERE extractor='Todos'
AND nombre_id='ID_CANAL') d
ON UPPER(a.canal_comercial)=UPPER(d.tipo_movimiento)
LEFT JOIN (SELECT DISTINCT tipo_movimiento,id_tipo_movimiento 
FROM db_reportes.otc_t_catalogo_consolidado_id
WHERE extractor='Todos'
AND nombre_id='ID_SUBCANAL') e
ON UPPER(CASE WHEN a.canal_comercial='NEGOCIOS INDIRECTOS' THEN 'DISTRIBUIDOR PYMES' ELSE 'CC COMERCIAL CALLCENTER B2B' END)=UPPER(e.tipo_movimiento)) a

""".format(vTexto=vTexto,vFecha_Proceso=vFecha_Proceso)

print(vSql)
timestart_tbl = datetime.datetime.now()
print ("==== Guardando los datos en tabla "+bd+" ====")
df0 = spark.sql(vSql.format(vFecha_Proceso=vFecha_Proceso))
print("Conteo: ",df0.count())
df0.repartition(10).write.mode("overwrite").insertInto(bd, overwrite=True)
df0.printSchema()
timeend_tbl = datetime.datetime.now()
duracion_tbl = timeend_tbl - timestart_tbl
print("Escritura Exitosa de la tabla "+bd)
print("Duracion create "+bd+" {}".format(duracion_tbl))

spark.stop()
timeend = datetime.datetime.now()
duracion = timeend - timestart
print("Duracion: {vDuracion}".format(vDuracion=duracion))



