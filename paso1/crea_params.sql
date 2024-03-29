--CREA PARAMETROS PARA LA ENTIDAD D_TESTSQLSERVER EN params_des
DELETE FROM params_des WHERE ENTIDAD= 'D_TESTSQLSERVER';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TESTSQLSERVER','VAL_RUTA','/home/nae105215/RAW/USERDAS/OTC_T_V_BAJAS_SERVICIOS','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TESTSQLSERVER','VAL_ESQUEMA','db_desarrollo2021','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TESTSQLSERVER','VAL_TABLA','otc_t_v_bajas_servicios','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TESTSQLSERVER','VAL_DATABASE','telefonica','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TESTSQLSERVER','VAL_USUARIO','userdas','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TESTSQLSERVER','VAL_PASSWORD','CM3wm#$9iiHr','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TESTSQLSERVER','VAL_ESQUEMA_SQLSERVER','dbo','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TESTSQLSERVER','VAL_TABLA_SQLSERVER','v_bajas_servicios_DAS','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TESTSQLSERVER','VAL_URL','jdbc:sqlserver://10.122.7.242:14335;databaseName=telefonica','0','0');
SELECT * FROM params_des WHERE ENTIDAD='D_TESTSQLSERVER' ORDER BY PARAMETRO;

--CREA PARAMETROS PARA LA ENTIDAD D_URMDGTGNDS0020 EN params_des
DELETE FROM params_des WHERE ENTIDAD= 'D_URMDGTGNDS0020';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMDGTGNDS0020','VAL_RUTA','/home/nae105215/RAW/USERDAS/OTC_T_V_DIGITALES_GANADAS','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMDGTGNDS0020','VAL_ESQUEMA','db_desarrollo2021','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMDGTGNDS0020','VAL_TABLA','otc_t_v_digitales_ganadas','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMDGTGNDS0020','VAL_URL','jdbc:sqlserver://10.122.7.242:14335;databaseName=telefonica','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMDGTGNDS0020','VAL_DATABASE','telefonica','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMDGTGNDS0020','VAL_USUARIO','userdas','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMDGTGNDS0020','VAL_PASSWORD','CM3wm#$9iiHr','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMDGTGNDS0020','VAL_ESQUEMA_SQLSERVER','dbo','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMDGTGNDS0020','VAL_TABLA_SQLSERVER','v_digitales_ganadas_DAS','0','0');
select * from params_des WHERE ENTIDAD='D_URMDGTGNDS0020' order by PARAMETRO;

--CREA PARAMETROS PARA LA ENTIDAD D_URMTRDCNLIMPLMNTD0040 EN params_des
DELETE FROM params_des WHERE ENTIDAD= 'D_URMTRDCNLIMPLMNTD0040';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRDCNLIMPLMNTD0040','VAL_RUTA','/home/nae105215/RAW/USERDAS/OTC_T_V_TRADICIONALES_IMPLEMENTADAS','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRDCNLIMPLMNTD0040','VAL_ESQUEMA','db_desarrollo2021','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRDCNLIMPLMNTD0040','VAL_TABLA','otc_t_v_tradicionales_implementadas','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRDCNLIMPLMNTD0040','VAL_URL','jdbc:sqlserver://10.122.7.242:14335;databaseName=telefonica','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRDCNLIMPLMNTD0040','VAL_DATABASE','telefonica','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRDCNLIMPLMNTD0040','VAL_USUARIO','userdas','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRDCNLIMPLMNTD0040','VAL_PASSWORD','CM3wm#$9iiHr','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRDCNLIMPLMNTD0040','VAL_ESQUEMA_SQLSERVER','dbo','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRDCNLIMPLMNTD0040','VAL_TABLA_SQLSERVER','v_tradicionales_implementadas_DAS','0','0');
select * from params_des WHERE ENTIDAD='D_URMTRDCNLIMPLMNTD0040' order by PARAMETRO;

--CREA PARAMETROS PARA LA ENTIDAD D_URMTRDCNLSGNDS0030 EN params_des
DELETE FROM params_des WHERE ENTIDAD= 'D_URMTRDCNLSGNDS0030';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRDCNLSGNDS0030','VAL_RUTA','/home/nae105215/RAW/USERDAS/OTC_T_V_TRADICIONALES_GANADAS','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRDCNLSGNDS0030','VAL_ESQUEMA','db_desarrollo2021','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRDCNLSGNDS0030','VAL_TABLA','otc_t_v_tradicionales_ganadas','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRDCNLSGNDS0030','VAL_URL','jdbc:sqlserver://10.122.7.242:14335;databaseName=telefonica','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRDCNLSGNDS0030','VAL_DATABASE','telefonica','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRDCNLSGNDS0030','VAL_USUARIO','userdas','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRDCNLSGNDS0030','VAL_PASSWORD','CM3wm#$9iiHr','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRDCNLSGNDS0030','VAL_ESQUEMA_SQLSERVER','dbo','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRDCNLSGNDS0030','VAL_TABLA_SQLSERVER','v_tradicionales_ganadas_DAS','0','0');
SELECT * FROM params_des WHERE ENTIDAD='D_URMTRDCNLSGNDS0030' ORDER BY PARAMETRO;

--CREA PARAMETROS PARA LA ENTIDAD D_URMDGTIMPLMNTDS0010 EN params_des
DELETE FROM params_des WHERE ENTIDAD= 'D_URMDGTIMPLMNTDS0010';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMDGTIMPLMNTDS0010','VAL_RUTA','/home/nae105215/RAW/USERDAS/OTC_T_V_DIGITALES_IMPLEMENTADAS','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMDGTIMPLMNTDS0010','VAL_ESQUEMA','db_desarrollo2021','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMDGTIMPLMNTDS0010','VAL_TABLA','otc_t_v_digitales_implementadas','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMDGTIMPLMNTDS0010','VAL_URL','jdbc:sqlserver://10.122.7.242:14335;databaseName=telefonica','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMDGTIMPLMNTDS0010','VAL_DATABASE','CAPA_SEMANTICA','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMDGTIMPLMNTDS0010','VAL_USUARIO','OTECEL/NAE108834','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMDGTIMPLMNTDS0010','VAL_PASSWORD','FearofablankP42','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMDGTIMPLMNTDS0010','VAL_ESQUEMA_SQLSERVER','dbo','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMDGTIMPLMNTDS0010','VAL_TABLA_SQLSERVER','v_digitales_implementadas_DAS','0','0');
SELECT * FROM params_des WHERE ENTIDAD='D_URMDGTIMPLMNTDS0010' ORDER BY PARAMETRO;

--CREACION DE PARAMETROS EN params_des PARA LA SHELL PRINCIPAL DE CONECTIVIDAD
DELETE FROM params_des WHERE ENTIDAD= 'D_EXTRCNCTVDD0040';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRCNCTVDD0040','PARAM1_FECHA','date_format(sysdate(),''%Y%m%d'')','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRCNCTVDD0040','PARAM2_VAL_RUTA','''/home/nae105215/EXT_CONECTIVIDAD''','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRCNCTVDD0040','ETAPA','1','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRCNCTVDD0040','SHELL','/home/nae105215/EXT_CONECTIVIDAD/bin/OTC_T_EXT_CONECTIVIDAD.sh','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRCNCTVDD0040','VAL_ESQUEMA','db_desarrollo2021','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRCNCTVDD0040','VAL_TABLA','otc_t_ext_conectividad','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRCNCTVDD0040','VAL_NOM_ARCHIVO','Extractor_Conectividad.txt','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRCNCTVDD0040','VAL_FTP_PUERTO','22','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRCNCTVDD0040','VAL_FTP_USER','telefonicaecuadorprod','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRCNCTVDD0040','VAL_FTP_HOSTNAME','ftp.cloud.varicent.com','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRCNCTVDD0040','VAL_FTP_PASS','RqiZ2lkJmeiQTi2hvRwd','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRCNCTVDD0040','VAL_FTP_RUTA','/Data','0','0');
SELECT * FROM params_des WHERE ENTIDAD='D_EXTRCNCTVDD0040' ORDER BY PARAMETRO;







--CREA PARAMETROS PARA LA ENTIDAD D_TESTSQLSERVER EN params_des
DELETE FROM params_des WHERE ENTIDAD= 'D_TESTSQLSERVER';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TESTSQLSERVER','VAL_RUTA','/home/nae108834/sqlservertest','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TESTSQLSERVER','VAL_ESQUEMA','db_desarrollo2021','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TESTSQLSERVER','VAL_TABLA','otc_t_v_sqlservertest','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TESTSQLSERVER','VAL_DATABASE','Capa_Semantica','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TESTSQLSERVER','VAL_USUARIO','softconsulting','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TESTSQLSERVER','VAL_PASSWORD','Softconsulting2024','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TESTSQLSERVER','VAL_ESQUEMA_SQLSERVER','dbo','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TESTSQLSERVER','VAL_TABLA_SQLSERVER','HEC_INTERCONEXION','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TESTSQLSERVER','VAL_URL','jdbc:sqlserver://10.112.157.52:1433;databaseName=Capa_Semantica','0','0');
SELECT * FROM params_des WHERE ENTIDAD='D_TESTSQLSERVER' ORDER BY PARAMETRO;
