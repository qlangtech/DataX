package com.alibaba.datax.plugin.rdbms.writer.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.druid.sql.parser.ParserException;
import com.qlangtech.tis.plugin.ds.IDBReservedKeys;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class WriterUtil {
    private static final Logger LOG = LoggerFactory.getLogger(WriterUtil.class);

    //TODO 切分报错
    public static List<Configuration> doSplit(Configuration simplifiedConf, int adviceNumber,
                                              IDBReservedKeys reservedKeys) {

        List<Configuration> splitResultConfigs = new ArrayList<Configuration>();

        int tableNumber = simplifiedConf.getInt(Constant.TABLE_NUMBER_MARK);

        //处理单表的情况
        if (tableNumber == 1) {
            //由于在之前的  master prepare 中已经把 table,jdbcUrl 提取出来，所以这里处理十分简单
            for (int j = 0; j < adviceNumber; j++) {
                splitResultConfigs.add(simplifiedConf.clone());
            }

            return splitResultConfigs;
        }

        if (tableNumber != adviceNumber) {
            throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR, String.format("您的配置文件中的列配置信息有误. " +
                    "您要写入的目的端的表个数是:%s , 但是根据系统建议需要切分的份数是：%s. 请检查您的配置并作出修改.", tableNumber, adviceNumber));
        }

        String jdbcUrl;
//        List<String> preSqls = simplifiedConf.getList(Key.PRE_SQL, String.class);
//        List<String> postSqls = simplifiedConf.getList(Key.POST_SQL, String.class);

        List<Object> conns = simplifiedConf.getList(Constant.CONN_MARK, Object.class);
        SelectTable selTable = null;
        for (Object conn : conns) {
            Configuration sliceConfig = simplifiedConf.clone();

            Configuration connConf = Configuration.from(conn.toString());
            jdbcUrl = connConf.getString(Key.JDBC_URL);
            sliceConfig.set(Key.JDBC_URL, jdbcUrl);

            sliceConfig.remove(Constant.CONN_MARK);

            List<String> tables = connConf.getList(Key.TABLE, String.class);
            for (String table : tables) {

                selTable = SelectTable.create(table, reservedKeys);

                Configuration tempSlice = sliceConfig.clone();
                tempSlice.set(Key.TABLE, table);
//                tempSlice.set(Key.PRE_SQL, renderPreOrPostSqls(preSqls, selTable));
//                tempSlice.set(Key.POST_SQL, renderPreOrPostSqls(postSqls, selTable));

                splitResultConfigs.add(tempSlice);
            }

        }

        return splitResultConfigs;
    }

//    public static List<String> renderPreOrPostSqls(List<String> preOrPostSqls, SelectTable selTable) {
//        if (null == preOrPostSqls) {
//            return Collections.emptyList();
//        }
//
//        List<String> renderedSqls = new ArrayList<String>();
//        for (String sql : preOrPostSqls) {
//            //preSql为空时，不加入执行队列
//            if (StringUtils.isNotBlank(sql)) {
//                renderedSqls.add(sql.replace(Constant.TABLE_NAME_PLACEHOLDER, selTable.getTabName()));
//            }
//        }
//
//        return renderedSqls;
//    }

    public static void executeSqls(Connection conn, List<String> sqls, String basicMessage, DataBaseType dataBaseType) {

        String currentSql = null;

        try (Statement stmt = conn.createStatement()) {
            for (String sql : sqls) {
                currentSql = sql;
                DBUtil.executeSqlWithoutResultSet(stmt, sql);
            }
        } catch (Exception e) {
            throw RdbmsException.asQueryException(dataBaseType, e, currentSql, null, null);
        }
    }

    public static String getWriteTemplate(SelectCols columnHolders, List<String> valueHolders, String writeMode,
                                          DataBaseType dataBaseType, boolean forceUseUpdate) {
        boolean isWriteModeLegal =
                writeMode.trim().toLowerCase().startsWith("insert") || writeMode.trim().toLowerCase().startsWith(
                        "replace") || writeMode.trim().toLowerCase().startsWith("update");

        if (!isWriteModeLegal) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                    String.format("您所配置的 " + "writeMode" + ":%s 错误. 因为DataX 目前仅支持replace,update 或 insert 方式. " +
                            "请检查您的配置并作出修改.", writeMode));
        }
        // && writeMode.trim().toLowerCase().startsWith("replace")
        String writeDataSqlTemplate;
        if (forceUseUpdate || ((dataBaseType == DataBaseType.MySql || dataBaseType == DataBaseType.Tddl) && writeMode.trim().toLowerCase().startsWith("update"))) {
            //update只在mysql下使用

            // 添加分行符，这样在SQL执行错误时可以提供行号时候方便定位错误信息
            writeDataSqlTemplate =
                    new StringBuilder().append("INSERT INTO %s (")
                            .append(StringUtils.join(columnHolders, ",")).append(")  VALUES(")
                            .append(StringUtils.join(valueHolders, ",")).append(")").append(columnHolders.onDuplicateKeyUpdateString()).toString();
        } else {

            //这里是保护,如果其他错误的使用了update,需要更换为replace
            if (writeMode.trim().toLowerCase().startsWith("update")) {
                writeMode = "replace";
            }
            writeDataSqlTemplate =
                    new StringBuilder().append(writeMode).append(" INTO %s (").append(columnHolders.getCols())
                            //StringUtils.join(columnHolders, ",")
                            .append(") VALUES(").append(StringUtils.join(valueHolders, ",")).append(")").toString();
        }

        return writeDataSqlTemplate;
    }

    public static void preCheckPrePareSQL(Configuration originalConfig, DataBaseType type, IDBReservedKeys escapeChar) {
        //        List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
        //        Configuration connConf = Configuration.from(conns.get(0).toString());
        //        String table = connConf.getList(Key.TABLE, String.class).get(0);

     //   SelectTable selTab = SelectTable.create(originalConfig, escapeChar);

       // List<String> preSqls = originalConfig.getList(Key.PRE_SQL, String.class);
      //  List<String> renderedPreSqls = WriterUtil.renderPreOrPostSqls(preSqls, selTab);

//        if (null != renderedPreSqls && !renderedPreSqls.isEmpty()) {
//            LOG.info("Begin to preCheck preSqls:[{}].", StringUtils.join(renderedPreSqls, ";"));
//            for (String sql : renderedPreSqls) {
//                try {
                //    DBUtil.sqlValid(sql, type);
//                } catch (ParserException e) {
//                    throw RdbmsException.asPreSQLParserException(type, e, sql);
//                }
//            }
//        }
    }

    public static void preCheckPostSQL(Configuration originalConfig, DataBaseType type, IDBReservedKeys escapeChar) {
        //        List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
        //        Configuration connConf = Configuration.from(conns.get(0).toString());
        //        String table = connConf.getList(Key.TABLE, String.class).get(0);

       // SelectTable selTab = SelectTable.create(originalConfig, escapeChar);

      //  List<String> postSqls = originalConfig.getList(Key.POST_SQL, String.class);
       // List<String> renderedPostSqls = WriterUtil.renderPreOrPostSqls(postSqls, selTab);
//        if (null != renderedPostSqls && !renderedPostSqls.isEmpty()) {
//
//            LOG.info("Begin to preCheck postSqls:[{}].", StringUtils.join(renderedPostSqls, ";"));
//            for (String sql : renderedPostSqls) {
//                try {
//                    DBUtil.sqlValid(sql, type);
//                } catch (ParserException e) {
//                    throw RdbmsException.asPostSQLParserException(type, e, sql);
//                }
//
//            }
//        }
    }


}
