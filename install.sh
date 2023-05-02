mvn install -Dmaven.test.skip=true -fn -pl  \
hbase20xsqlreader\
,common-scala\
,plugin-unstructured-storage-util\
,odpswriter\
,elasticsearchwriter\
,hdfsreader\
,hdfswriter\
,mysqlreader\
,mysqlwriter\
,osswriter\
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
,mongodbwriter \
-am

