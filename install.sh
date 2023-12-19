mvn clean install -Dmaven.test.skip=true \
-Ptis-repo \
-pl  \
hbase20xsqlreader\
,plugin-unstructured-storage-util\
,odpswriter\
,elasticsearchwriter\
,mysqlreader\
,mysqlwriter\
,postgresqlreader\
,postgresqlwriter\
,plugin-rdbms-util\
,doriswriter\
,starrockswriter\
,oraclereader\
,oraclewriter\
,clickhousewriter\
,sqlserverreader\
,sqlserverwriter\
,streamreader\
,streamwriter\
,hdfsreader\
,hdfswriter\
,ftpreader\
,ftpwriter\
,cassandrareader\
,cassandrawriter\
,mongodbreader\
,mongodbwriter\
,kingbaseesreader\
,kingbaseeswriter \
-am -fn

