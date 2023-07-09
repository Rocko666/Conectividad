set -e
#########################################################################################################
# NOMBRE: OTC_T_V_TRADICIONALES_GANADAS.sh		     	      								            #
# DESCRIPCION:																							#
#   Shell que extrae la información de la tabla v_tradicionales_ganadas de SQLServer a Hive				#
# AUTOR: Karina Castro - Softconsulting                            										#
# FECHA CREACION: 2022-09-12   																			#
# PARAMETROS DEL SHELL                            													    #
# N/A  												    						                        #
#########################################################################################################
# MODIFICACIONES																						#
# FECHA  		AUTOR     		DESCRIPCION MOTIVO														#
#########################################################################################################
##############
# VARIABLES #
##############
ENTIDAD=D_URMTRDCNLSGNDS0030
version=1.2.1000.2.6.4.0-91
HADOOP_CLASSPATH=$(hcat -classpath) export HADOOP_CLASSPATH

###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros del SPARK GENERICO" 
###########################################################################################################################################################
VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$VAL_KINIT

#PARAMETROS GENERICOS
#VAL_COLA_EJECUCION=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_PARAM_BEELINE' AND parametro = 'VAL_COLA_EJECUCION';"`
VAL_COLA_EJECUCION='reportes'
VAL_CADENA_JDBC=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_PARAM_BEELINE' AND parametro = 'VAL_CADENA_JDBC';"`
#VAL_USER=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_USER';"`
VAL_USER=nae105215

#PARAMETROS QUE RECIBE LA SHELL
VAL_RUTA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_RUTA';"`
VAL_ESQUEMA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ESQUEMA';"`
VAL_TABLA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA';"`
VAL_URL=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_URL';"`
VAL_DATABASE=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DATABASE';"`
VAL_USUARIO=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_USUARIO';"`
VAL_PASSWORD=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_PASSWORD';"`
VAL_ESQUEMA_SQLSERVER=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ESQUEMA_SQLSERVER';"`
VAL_TABLA_SQLSERVER=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_SQLSERVER';"`
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_RUTA_SPARK';"`

#PARAMETROS CALCULADOS Y AUTOGENERADOS
VAL_BD=$VAL_ESQUEMA.$VAL_TABLA
VAL_BD_SQLSERVER=$VAL_ESQUEMA_SQLSERVER.$VAL_TABLA_SQLSERVER
VAL_DIA=`date '+%Y%m%d'` 
VAL_HORA=`date '+%H%M%S'` 
VAL_LOG=$VAL_RUTA/log/OTC_T_V_TRADICIONALES_GANADAS_$VAL_DIA$VAL_HORA.log

#VALIDACION DE PARAMETROS INICIALES
if  [ -z "$ENTIDAD" ] || 
    [ -z "$VAL_RUTA" ] || 
    [ -z "$VAL_ESQUEMA" ] || 
    [ -z "$VAL_TABLA" ] || 
    [ -z "$VAL_URL" ] || 
    [ -z "$VAL_DATABASE" ] || 
    [ -z "$VAL_USUARIO" ] || 
    [ -z "$VAL_PASSWORD" ] || 
    [ -z "$VAL_ESQUEMA_SQLSERVER" ] || 
    [ -z "$VAL_TABLA_SQLSERVER" ] || 
    [ -z "$VAL_RUTA_SPARK" ] || 
    [ -z "$VAL_LOG" ] || 
    [ -z "$VAL_COLA_EJECUCION" ] || 
    [ -z "$VAL_CADENA_JDBC" ] || 
    [ -z "$VAL_USER" ]; then
	echo " ERROR: - uno de los parametros esta vacio o nulo"
	exit 1
fi

#INICIO DEL PROCESO
echo "==== Inicia extraccion tabla V_TRADICIONALES_GANADAS_DAS de SQLServer ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG

#REALIZA LA TRANSFERENCIA DE LOS ARCHIVOS DESDE EL SERVIDOR FTP A RUTA LOCAL EN BIGDATA
echo "==== Ejecuta archivo spark otc_t_v_tradicionales_ganadas.py que extrae informacion de Postgress a Hive ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
$VAL_RUTA_SPARK \
--conf spark.ui.enabled=false \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.port.maxRetries=100 \
--master local \
--executor-memory 32G \
--num-executors 8 \
--executor-cores 4 \
--driver-memory 32G \
--jars /var/opt/tel_lib/sqljdbc42.jar \
$VAL_RUTA/python/otc_t_v_tradicionales_ganadas.py \
--vUrl=$VAL_URL \
--vDatabase=$VAL_DATABASE \
--vUid=$VAL_USUARIO \
--bd=$VAL_BD \
--vPwd=$VAL_PASSWORD \
--vTabla=$VAL_BD_SQLSERVER 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'SyntaxError:|pyodbc.InterfaceError:|Caused by:|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark otc_t_v_tradicionales_ganadas.py es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
	else
	echo "==== ERROR: - En la ejecucion del archivo spark otc_t_v_tradicionales_ganadas.py ====" 2>&1 &>> $VAL_LOG
	exit 1
fi

#VALIDA EXTRACCION DE REGISTROS
echo "==== Verifica que se hayan extraido registros en Hive ====" 2>&1 &>> $VAL_LOG
cant_reg_d=$(beeline -u $VAL_CADENA_JDBC -n $VAL_USER --hiveconf tez.queue.name=$VAL_COLA_EJECUCION --showHeader=false --outputformat=tsv2 -e "SELECT COUNT(1) FROM $VAL_ESQUEMA.$VAL_TABLA;")
echo "Cantidad registros destino: $cant_reg_d" 2>&1 &>> $VAL_LOG
	if [ $cant_reg_d -gt 0 ]; then
			echo "==== OK - Se extrajeron datos de la fuente ====" 2>&1 &>> $VAL_LOG
		else
			echo "==== ERROR: - No se extrajeron datos de la fuente ====" 2>&1 &>> $VAL_LOG
			exit 1
	fi

echo "==== Finaliza extraccion tabla V_TRADICIONALES_GANADAS_DAS de SQLServer ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG