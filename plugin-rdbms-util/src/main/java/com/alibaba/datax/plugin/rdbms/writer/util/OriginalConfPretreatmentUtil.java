package com.alibaba.datax.plugin.rdbms.writer.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.*;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.IDataSourceFactoryGetter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class OriginalConfPretreatmentUtil {
    private static final Logger LOG = LoggerFactory
            .getLogger(OriginalConfPretreatmentUtil.class);

    public static DataBaseType DATABASE_TYPE;

//    public static void doPretreatment(Configuration originalConfig) {
//        doPretreatment(originalConfig,null);
//    }

    public static void doPretreatment(Configuration originalConfig, IDataSourceFactoryGetter dataSourceFactoryGetter, DataBaseType dataBaseType) {
        // 检查 username/password 配置（必填）
        originalConfig.getNecessaryValue(Key.USERNAME, DBUtilErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.PASSWORD, DBUtilErrorCode.REQUIRED_VALUE);

        doCheckBatchSize(originalConfig);

        simplifyConf(originalConfig);

        dealColumnConf(dataSourceFactoryGetter, originalConfig);
        dealWriteMode(originalConfig, dataBaseType, dataSourceFactoryGetter);
    }

    public static void doCheckBatchSize(Configuration originalConfig) {
        // 检查batchSize 配置（选填，如果未填写，则设置为默认值）
        int batchSize = originalConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
        if (batchSize < 1) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE, String.format(
                    "您的batchSize配置有误. 您所配置的写入数据库表的 batchSize:%s 不能小于1. 推荐配置范围为：[100-1000], 该值越大, 内存溢出可能性越大. 请检查您的配置并作出修改.",
                    batchSize));
        }

        originalConfig.set(Key.BATCH_SIZE, batchSize);
    }

    public static void simplifyConf(Configuration originalConfig) {
        List<Object> connections = originalConfig.getList(Constant.CONN_MARK,
                Object.class);

        int tableNum = 0;

        for (int i = 0, len = connections.size(); i < len; i++) {
            Configuration connConf = Configuration.from(connections.get(i).toString());

            String jdbcUrl = connConf.getString(Key.JDBC_URL);
            if (StringUtils.isBlank(jdbcUrl)) {
                throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE, "您未配置的写入数据库表的 jdbcUrl.");
            }

            jdbcUrl = DATABASE_TYPE.appendJDBCSuffixForWriter(jdbcUrl);
            originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, i, Key.JDBC_URL),
                    jdbcUrl);

            List<String> tables = connConf.getList(Key.TABLE, String.class);

            if (null == tables || tables.isEmpty()) {
                throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE,
                        "您未配置写入数据库表的表名称. 根据配置DataX找不到您配置的表. 请检查您的配置并作出修改.");
            }

            // 对每一个connection 上配置的table 项进行解析
            List<String> expandedTables = TableExpandUtil
                    .expandTableConf(DATABASE_TYPE, tables);

            if (null == expandedTables || expandedTables.isEmpty()) {
                throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                        "您配置的写入数据库表名称错误. DataX找不到您配置的表，请检查您的配置并作出修改.");
            }

            tableNum += expandedTables.size();

            originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK,
                    i, Key.TABLE), expandedTables);
        }

        originalConfig.set(Constant.TABLE_NUMBER_MARK, tableNum);
    }

    public static void dealColumnConf(Configuration originalConfig, ConnectionFactory connectionFactory, SelectTable oneTable) {
        //SelectCols userConfiguredColumns =  SelectCols.createSelectCols( originalConfig.getList(Key.COLUMN, String.class) ,originalConfig.get(Key.ESCAPE_CHAR, String.class) );
        SelectCols userConfiguredColumns = SelectCols.createSelectCols(originalConfig, connectionFactory.getDSEntityEscape());
//        if (null == userConfiguredColumns || userConfiguredColumns.isEmpty()) {
//            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
//                    "您的配置文件中的列配置信息有误. 因为您未配置写入数据库表的列名称，DataX获取不到列信息. 请检查您的配置并作出修改.");
//        } else {
        boolean isPreCheck = originalConfig.getBool(Key.DRYRUN, false);
        List<String> allColumns;
        if (isPreCheck) {
            allColumns = DBUtil.getTableColumnsByConn(DATABASE_TYPE, true, connectionFactory.getConnecttionWithoutRetry(), oneTable, connectionFactory.getConnectionInfo());
        } else {
            allColumns = DBUtil.getTableColumnsByConn(DATABASE_TYPE, true, connectionFactory.getConnecttionWithoutRetry(), oneTable, connectionFactory.getConnectionInfo());
        }

        LOG.info("table:[{}] all columns:[\n{}\n].", oneTable,
                StringUtils.join(allColumns, ","));

        if (userConfiguredColumns.isSelectAllCols()) {
            LOG.warn("您的配置文件中的列配置信息存在风险. 因为您配置的写入数据库表的列为*，当您的表字段个数、类型有变动时，可能影响任务正确性甚至会运行出错。请检查您的配置并作出修改.");

            // 回填其值，需要以 String 的方式转交后续处理
            originalConfig.set(Key.COLUMN, allColumns);
        } else if (userConfiguredColumns.size() > allColumns.size()) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                    String.format("您的配置文件中的列配置信息有误. 因为您所配置的写入数据库表的字段个数:%s 大于目的表的总字段总个数:%s. 请检查您的配置并作出修改.",
                            userConfiguredColumns.size(), allColumns.size()));
        } else {
            // 确保用户配置的 column 不重复
            userConfiguredColumns.makeSureNoValueDuplicate(false);

            // 检查列是否都为数据库表中正确的列（通过执行一次 select column from table 进行判断）
            String cfgCols = StringUtils.join(userConfiguredColumns, ",");
            List<ColumnMetaData> cols = DBUtil.getColumnMetaData(connectionFactory.getConnecttionWithoutRetry(), true, oneTable, userConfiguredColumns);

            if (cols.size() != userConfiguredColumns.size()) {
                List<String> existCols = cols.stream().map((c) -> c.getName()).collect(Collectors.toList());
                throw new IllegalStateException("db table:" + oneTable + " exist cols:"
                        + StringUtils.join(existCols, ",") + " not equal with config cols:" + cfgCols);
            }
        }
        // }
    }

    public static void dealColumnConf(IDataSourceFactoryGetter dataSourceFactoryGetter, Configuration originalConfig) {
        String jdbcUrl = originalConfig.getString(String.format("%s[0].%s",
                Constant.CONN_MARK, Key.JDBC_URL));

        String username = originalConfig.getString(Key.USERNAME);
        String password = originalConfig.getString(Key.PASSWORD);
        SelectTable oneTable = SelectTableUtils.create(originalConfig, dataSourceFactoryGetter.getDBReservedKeys());
//        .getString(String.format(
//                "%s[0].%s[0]", Constant.CONN_MARK, Key.TABLE));

        JdbcConnectionFactory jdbcConnectionFactory = new JdbcConnectionFactory(dataSourceFactoryGetter, jdbcUrl, username, password);
        dealColumnConf(originalConfig, jdbcConnectionFactory, oneTable);
    }

    public static void dealWriteMode(Configuration originalConfig, DataBaseType dataBaseType, IDataSourceFactoryGetter dataSourceFactoryGetter) {
        SelectCols columns = SelectCols.createSelectCols(originalConfig, dataSourceFactoryGetter.getDataSourceFactory());

        String jdbcUrl = originalConfig.getString(String.format("%s[0].%s",
                Constant.CONN_MARK, Key.JDBC_URL, String.class));

        // 默认为：insert 方式
        String writeMode = originalConfig.getString(Key.WRITE_MODE, "INSERT");

        List<String> valueHolders = new ArrayList<String>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            valueHolders.add("?");
        }

        boolean forceUseUpdate = false;
        //ob10的处理
        if (dataBaseType == DataBaseType.MySql && isOB10(jdbcUrl)) {
            forceUseUpdate = true;
        }

        String writeDataSqlTemplate = WriterUtil.getWriteTemplate(columns, valueHolders, writeMode, dataBaseType, forceUseUpdate);

        LOG.info("Write data [\n{}\n], which jdbcUrl like:[{}]", writeDataSqlTemplate, jdbcUrl);

        originalConfig.set(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK, writeDataSqlTemplate);
    }

    public static boolean isOB10(String jdbcUrl) {
        //ob10的处理
        if (jdbcUrl.startsWith(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING)) {
            String[] ss = jdbcUrl.split(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING_PATTERN);
            if (ss.length != 3) {
                throw DataXException
                        .asDataXException(
                                DBUtilErrorCode.JDBC_OB10_ADDRESS_ERROR, "JDBC OB10格式错误，请联系askdatax");
            }
            return true;
        }
        return false;
    }

}
