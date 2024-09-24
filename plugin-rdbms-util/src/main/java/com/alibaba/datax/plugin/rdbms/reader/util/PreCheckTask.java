package com.alibaba.datax.plugin.rdbms.reader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.IJobContainerContext;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import com.alibaba.druid.sql.parser.ParserException;
import com.qlangtech.tis.plugin.ds.IDataSourceFactoryGetter;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by judy.lt on 2015/6/4.
 */
public class PreCheckTask implements Callable<Boolean> {
    private static final Logger LOG = LoggerFactory.getLogger(PreCheckTask.class);
    private String userName;
    private String password;
    private String splitPkId;
    private Configuration connection;
    private DataBaseType dataBaseType;
    private final IDataSourceFactoryGetter dataSourceFactoryGetter;
    private final IJobContainerContext containerContext;

    public PreCheckTask(IDataSourceFactoryGetter dataSourceFactoryGetter, String userName,
                        String password,
                        Configuration connection,
                        DataBaseType dataBaseType,
                        String splitPkId,
                        IJobContainerContext containerContext) {
        this.connection = connection;
        this.userName = userName;
        this.password = password;
        this.dataBaseType = dataBaseType;
        this.splitPkId = splitPkId;
        this.dataSourceFactoryGetter = dataSourceFactoryGetter;
        this.containerContext = containerContext;
    }

    @Override
    public Boolean call() throws DataXException {
        String jdbcUrl = this.connection.getString(Key.JDBC_URL);
        List<Object> querySqls = this.connection.getList(Key.QUERY_SQL, Object.class);
        List<Object> splitPkSqls = this.connection.getList(Key.SPLIT_PK_SQL, Object.class);
        List<Object> tables = this.connection.getList(Key.TABLE, Object.class);
        Connection conn = DBUtil.getConnectionWithoutRetry(this.dataSourceFactoryGetter, jdbcUrl,
                this.userName, password);
        int fetchSize = 1;
        if (DataBaseType.MySql.equals(dataBaseType) || DataBaseType.DRDS.equals(dataBaseType)) {
            fetchSize = Integer.MIN_VALUE;
        }
        try {
            for (int i = 0; i < querySqls.size(); i++) {

                String splitPkSql = null;
                String querySql = querySqls.get(i).toString();

                String table = null;
                if (tables != null && !tables.isEmpty()) {
                    table = tables.get(i).toString();
                }

                /*verify query*/
                ResultSet rs = null;
                Statement statement = null;
                try {
                    DBUtil.sqlValid(querySql, dataBaseType);
                    if (i == 0) {
                        Pair<Statement, ResultSet> stat = DBUtil.query(conn, new QuerySql(querySql), fetchSize, dataSourceFactoryGetter, this.containerContext);
                        statement = stat.getLeft();
                        rs = stat.getRight();
                    }
                } catch (ParserException e) {
                    throw RdbmsException.asSqlParserException(this.dataBaseType, e, querySql);
                } catch (Exception e) {
                    throw RdbmsException.asQueryException(this.dataBaseType, e, querySql, table, userName);
                } finally {
                    DBUtil.closeDBResources(rs, statement, null);
                }
                /*verify splitPK*/
                try {
                    if (splitPkSqls != null && !splitPkSqls.isEmpty()) {
                        splitPkSql = splitPkSqls.get(i).toString();
                        DBUtil.sqlValid(splitPkSql, dataBaseType);
                        if (i == 0) {
                            SingleTableSplitUtil.precheckSplitPk(
                                    dataSourceFactoryGetter, containerContext, conn, splitPkSql, fetchSize, table, userName);
                        }
                    }
                } catch (ParserException e) {
                    throw RdbmsException.asSqlParserException(this.dataBaseType, e, splitPkSql);
                } catch (DataXException e) {
                    throw e;
                } catch (Exception e) {
                    throw RdbmsException.asSplitPKException(this.dataBaseType, e, splitPkSql, this.splitPkId.trim());
                }
            }
        } finally {
            DBUtil.closeDBResources(null, conn);
        }
        return true;
    }
}
