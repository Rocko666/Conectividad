set -e
#########################################################################################################
# NOMBRE: OTC_T_EXT_CONECTIVIDAD.sh	    		     	      								            #
# DESCRIPCION:																							#
#   Shell que realiza las transformaciones para insertar los datos en la tabla destino particionada en	#
#   Hive y luego genera archivo txt para exportar a servidor FTP.										#
# AUTOR: Karina Castro - Softconsulting                            										#
# FECHA CREACION: 2022-09-14   																			#
# PARAMETROS DEL SHELL                            													    #
# VAL_FECHA_EJEC=${1} 		Fecha de ejecucion de proceso en formato  YYYYMMDD                          #
# VAL_RUTA=${2} 			Directorio principal del requerimiento				                        #
#########################################################################################################
# MODIFICACIONES																						#
# FECHA  		AUTOR     		DESCRIPCION MOTIVO														#
#########################################################################################################
##############
# VARIABLES #
##############

ENTIDAD=EXTRCNCTVDD0040

#PARAMETROS QUE RECIBE LA SHELL
VAL_FECHA_EJEC=$1 
VAL_RUTA=$2 
ETAPA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ETAPA';"`
VAL_ESQUEMA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ESQUEMA';"`
VAL_TABLA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA';"`
VAL_NOM_ARCHIVO=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOM_ARCHIVO';"`
VAL_FTP_PUERTO=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_PUERTO';"`
VAL_FTP_USER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_USER';"`
VAL_FTP_HOSTNAME=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_HOSTNAME';"`
VAL_FTP_PASS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_PASS';"`
VAL_FTP_RUTA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_RUTA';"`

#PARAMETROS CALCULADOS Y AUTOGENERADOS
VAL_FEC_AYER=`date -d "${VAL_FECHA_EJEC} -1 day"  +"%Y%m%d"`
VAL_DIA_UNO=`date -d "${VAL_FEC_AYER} -1 day"  +"%Y%m01"`
VAL_FECHA_INI_PRE=`date -d "${VAL_DIA_UNO} -1 day"  +"%Y%m01"`
VAL_FECHA_PROCESO=`date -d "${VAL_DIA_UNO} -1 day"  +"%d/%m/%Y"`
VAL_YEAR=`echo $VAL_FECHA_INI_PRE | cut -c1-4`
VAL_MONTH=`echo $VAL_FECHA_INI_PRE | cut -c5-6`
VAL_DAY=`echo $VAL_FECHA_INI_PRE | cut -c7-8`
VAL_FECHA_INI_FRMT=$VAL_YEAR"-"$VAL_MONTH"-"$VAL_DAY" 00:00:00"
VAL_FECHA_INI=$VAL_FECHA_INI_FRMT
VAL_YEAR_FIN=`echo $VAL_DIA_UNO | cut -c1-4`
VAL_MONTH_FIN=`echo $VAL_DIA_UNO | cut -c5-6`
VAL_DAY_FIN=`echo $VAL_DIA_UNO | cut -c7-8`
VAL_FECHA_FIN_FRMT=$VAL_YEAR_FIN"-"$VAL_MONTH_FIN"-"$VAL_DAY_FIN" 00:00:00"
VAL_FECHA_FIN=$VAL_FECHA_FIN_FRMT
VAL_BD=$VAL_ESQUEMA.$VAL_TABLA
VAL_DIA=`date '+%Y%m%d'` 
VAL_HORA=`date '+%H%M%S'` 
VAL_LOG=$VAL_RUTA/log/OTC_T_EXT_CONECTIVIDAD_$VAL_DIA$VAL_HORA.log
VAL_RUTA_ARCHIVO=$VAL_RUTA/output/$VAL_NOM_ARCHIVO
VAL_RUTA_ARCHIVO_PRE=$VAL_RUTA"/output/PRE_"$VAL_NOM_ARCHIVO

###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros del SPARK GENERICO" 
###########################################################################################################################################################
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';"`
VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$VAL_KINIT

#VALIDACION DE PARAMETROS INICIALES
if  [ -z "$ENTIDAD" ] || 
    [ -z "$VAL_FECHA_EJEC" ] || 
    [ -z "$ETAPA" ] || 
    [ -z "$VAL_RUTA" ] || 
    [ -z "$VAL_ESQUEMA" ] || 
    [ -z "$VAL_TABLA" ] || 
    [ -z "$VAL_FECHA_INI" ] || 
    [ -z "$VAL_FECHA_FIN" ] || 
    [ -z "$VAL_RUTA_SPARK" ] || 
    [ -z "$VAL_NOM_ARCHIVO" ] || 
    [ -z "$VAL_FTP_PUERTO" ] || 
    [ -z "$VAL_FTP_USER" ] || 
    [ -z "$VAL_FTP_HOSTNAME" ] || 
    [ -z "$VAL_FTP_PASS" ] || 
    [ -z "$VAL_FTP_RUTA" ] ||  
    [ -z "$VAL_LOG" ]; then
	echo " ERROR: - uno de los parametros esta vacio o nulo"
	exit 1
fi

#INICIO DEL PROCESO MES ACTUAL
echo "==== Inicia ejecucion proceso EXTRACTOR CONECTIVIDAD ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
echo "Los parametros del proceso son los siguientes:" >> $VAL_LOG
echo "Fecha Ejecucion: $VAL_FECHA_EJEC" >> $VAL_LOG
echo "Fecha Proceso: $VAL_FECHA_PROCESO" >> $VAL_LOG
echo "Fecha Inicio: $VAL_FECHA_INI" >> $VAL_LOG
echo "Fecha Fin: $VAL_FECHA_FIN" >> $VAL_LOG
echo "Servidor: $VAL_FTP_HOSTNAME" >> $VAL_LOG
echo "Puerto: $VAL_FTP_PUERTO" >> $VAL_LOG
echo "Usuario: $VAL_FTP_USER" >> $VAL_LOG
echo "Clave: $VAL_FTP_PASS" >> $VAL_LOG
echo "Ruta: $VAL_FTP_RUTA" >> $VAL_LOG

#PASO 1: REALIZA EL LLAMADO AL ARCHIVO SPARK QUE EJECUTA LOGICA EN HIVE PARA GENERAR INFORMACION EN TABLA DESTINO
if [ "$ETAPA" = "1" ]; then
export PYTHONIOENCODING=UTF-8
export LC_ALL="en_US.UTF-8"
echo "==== Ejecuta archivo spark otc_t_ext_conectividad.py que carga informacion a Hive ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
$VAL_RUTA_SPARK \
--conf spark.port.maxRetries=100 \
--master yarn \
--queue reportes \
--executor-memory 16G \
--num-executors 12 \
--executor-cores 12 \
--driver-memory 16G \
$VAL_RUTA/python/otc_t_ext_conectividad.py \
--vFecha_Proceso=$VAL_FECHA_PROCESO \
--bd=$VAL_BD &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'KeyError:|AttributeError:|UnicodeDecodeError:|TypeError:|SyntaxError:|command not found|Caused by:|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark otc_t_ext_conectividad.py es EXITOSO ===="`date '+%H%M%S'` >> $VAL_LOG
	else
	echo "==== ERROR: - En la ejecucion del archivo spark otc_t_ext_conectividad.py ====" >> $VAL_LOG
	exit 1
fi
ETAPA=2
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 1 con EXITO ===="`date '+%H%M%S'` >> $VAL_LOG
`mysql -N  <<<"update params set valor='2' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#PASO 2: LEE TABLA DE EXTRACTOR CONECTIVIDAD Y GENERA ARCHIVO TXT EN RUTA OUTPUT
if [ "$ETAPA" = "2" ]; then
rm -f ${VAL_RUTA}/output/*
echo "==== Lee tabla de Extractor Conectividad y genera archivo en ruta output ====" >> $VAL_LOG
$VAL_RUTA_SPARK \
--conf spark.port.maxRetries=100 \
--master yarn \
--queue reportes \
--executor-memory 16G \
--num-executors 6 \
--executor-cores 6 \
--driver-memory 16G \
$VAL_RUTA/python/genera_archivo_txt.py \
--vTabla=$VAL_ESQUEMA"."$VAL_TABLA \
--vRuta=$VAL_RUTA_ARCHIVO_PRE \
--vFecha_Ini="$VAL_FECHA_INI" \
--vFecha_Fin="$VAL_FECHA_FIN" &>> $VAL_LOG

#CONVIERTE EL ARCHIVO A UTF-8
iconv -f windows-1252 -t UTF-8//TRANSLIT $VAL_RUTA_ARCHIVO_PRE -o $VAL_RUTA_ARCHIVO

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'KeyError:AttributeError:|Error PySpark:|error:|An error occurred|ERROR FileFormatWriter:|Traceback|SyntaxError|UnicodeDecodeError|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark genera_archivo_txt.py es EXITOSO ===="`date '+%H%M%S'` >> $VAL_LOG
	else
	echo "==== ERROR: - En la ejecucion del archivo spark genera_archivo_txt.py ====" >> $VAL_LOG
	exit 1
fi

#VERIFICA SI EL ARCHIVO TXT CONTIENE DATOS
echo "==== Valida si el archivo TXT contiene datos ====" >> $VAL_LOG
cant_reg=`wc -l ${VAL_RUTA_ARCHIVO}` 
echo $cant_reg >> $VAL_LOG
cant_reg=`echo ${cant_reg}|cut -f1 -d" "` 
cant_reg=`expr $cant_reg + 0` 
	if [ $cant_reg -ne 0 ]; then
			echo "==== OK - El archivo TXT contiene datos para transferir al servidor FTP ====" >> $VAL_LOG
		else
			echo "==== ERROR: - El archivo TXT no contiene datos para transferir al servidor FTP ====" >> $VAL_LOG
			exit 1
	fi
ETAPA=3
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 2 con EXITO ===="`date '+%H%M%S'` >> $VAL_LOG
`mysql -N  <<<"update params set valor='3' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#CREA FUNCION PARA LA EXPORTACION DEL ARCHIVO A RUTA FTP Y REALIZA LA TRANSFERENCIA
if [ "$ETAPA" = "3" ]; then
echo "==== Crea funcion para la exportacion del archivo a ruta FTP ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG

function exportar()
{
    /usr/bin/expect << EOF >> $VAL_LOG
		set timeout -1
		spawn sftp ${VAL_FTP_USER}@${VAL_FTP_HOSTNAME} ${VAL_FTP_PUERTO}
		expect "password:"
		send "${VAL_FTP_PASS}\n"
		expect "sftp>"
		send "cd ${VAL_FTP_RUTA}\n"
		expect "sftp>"
		send "put $VAL_RUTA_ARCHIVO\n"
		expect "sftp>"
		send "exit\n"
		interact
EOF
}

#REALIZA LA TRANSFERENCIA DEL ARCHIVO TXT A RUTA FTP
echo  "==== Inicia exportacion del archivo TXT a servidor $VAL_FTP_HOSTNAME ====" >> $VAL_LOG
echo "Host SFTP: $VAL_FTP_HOSTNAME" >> $VAL_LOG
echo "Puerto SFTP: $VAL_FTP_PUERTO" >> $VAL_LOG
echo "Usuario SFTP: $VAL_FTP_USER" >> $VAL_LOG
echo "Password SFTP: $VAL_FTP_PASS" >> $VAL_LOG
echo "Ruta SFTP: $VAL_FTP_RUTA" >> $VAL_LOG
exportar $VAL_NOM_ARCHIVO &>> $VAL_LOG

#VALIDA EJECUCION DE LA TRANSFERENCIA DEL ARCHIVO TXT A RUTA FTP
echo "==== Valida transferencia del archivo TXT al servidor FTP ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
VAL_ERROR_FTP=`egrep 'Connection timed out|Not connected|syntax is incorrect|cannot find|There is not enough space|cannot find the file specified|Permission denied|No such file or directory|cannot access' $VAL_LOG | wc -l`
	if [ $VAL_ERROR_FTP -ne 0 ]; then
		echo "==== ERROR - En la transferencia del archivo TXT al servidor FTP ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
		exit 1
		else
		echo "==== OK - La transferencia del archivo TXT al servidor FTP es EXITOSA ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
	fi
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 3 con EXITO ===="`date '+%H%M%S'` >> $VAL_LOG
`mysql -N  <<<"update params set valor='1' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

echo "==== Finaliza ejecucion proceso EXTRACTOR CONECTIVIDAD ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
