package com.alibaba.datax.plugin.writer.clickhousewriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.util.IStatementSetter;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DataType;

import java.sql.*;
import java.util.List;
import java.util.regex.Pattern;

public class ClickhouseWriter extends Writer {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.ClickHouse;

    public static class Job extends Writer.Job {
        protected Configuration originalConfig = null;
        protected CommonRdbmsWriter.Job commonRdbmsWriterMaster;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.commonRdbmsWriterMaster = new CommonRdbmsWriter.Job(DATABASE_TYPE,this.containerContext);
            this.commonRdbmsWriterMaster.init(this.originalConfig);
        }

        @Override
        public void prepare() {
            this.commonRdbmsWriterMaster.prepare(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return this.commonRdbmsWriterMaster.split(this.originalConfig, mandatoryNumber);
        }

        @Override
        public void post() {
            this.commonRdbmsWriterMaster.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterMaster.destroy(this.originalConfig);
        }
    }

    public static class Task extends Writer.Task {
        protected Configuration writerSliceConfig;

        protected CommonRdbmsWriter.Task commonRdbmsWriterSlave;

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();

            this.commonRdbmsWriterSlave = new CommonRdbmsWriter.Task(DATABASE_TYPE,this.containerContext) {

//                @Override
//                protected PreparedStatement fillPreparedStatementColumnType(
//                        PreparedStatement preparedStatement, IStatementSetter col, int columnIndex, ColumnMetaData cm, Column column) throws SQLException {
//
//
//                    DataType type = cm.getType();
//                    try {
//
//                        if (column.getRawData() == null) {
//                            preparedStatement.setNull(columnIndex + 1, columnSqltype);
//                            return preparedStatement;
//                        }
//
//                        java.util.Date utilDate;
//                        switch (columnSqltype) {
//                            case Types.CHAR:
//                            case Types.NCHAR:
//                            case Types.CLOB:
//                            case Types.NCLOB:
//                            case Types.VARCHAR:
//                            case Types.LONGVARCHAR:
//                            case Types.NVARCHAR:
//                            case Types.LONGNVARCHAR:
//                                preparedStatement.setString(columnIndex + 1, column
//                                        .asString());
//                                break;
//
//                            case Types.TINYINT:
//                            case Types.SMALLINT:
//                            case Types.INTEGER:
//                            case Types.BIGINT:
//                            case Types.DECIMAL:
//                            case Types.FLOAT:
//                            case Types.REAL:
//                            case Types.DOUBLE:
//                                String strValue = column.asString();
//                                if (emptyAsNull && "".equals(strValue)) {
//                                    preparedStatement.setNull(columnIndex + 1, columnSqltype);
//                                } else {
//                                    switch (columnSqltype) {
//                                        case Types.TINYINT:
//                                        case Types.SMALLINT:
//                                        case Types.INTEGER:
//                                            preparedStatement.setInt(columnIndex + 1, column.asBigInteger().intValue());
//                                            break;
//                                        case Types.BIGINT:
//                                            preparedStatement.setLong(columnIndex + 1, column.asLong());
//                                            break;
//                                        case Types.DECIMAL:
//                                            preparedStatement.setBigDecimal(columnIndex + 1, column.asBigDecimal());
//                                            break;
//                                        case Types.REAL:
//                                        case Types.FLOAT:
//                                            preparedStatement.setFloat(columnIndex + 1, column.asDouble().floatValue());
//                                            break;
//                                        case Types.DOUBLE:
//                                            preparedStatement.setDouble(columnIndex + 1, column.asDouble());
//                                            break;
//                                    }
//                                }
//                                break;
//
//                            case Types.DATE:
//                                if (type.typeName
//                                        .equalsIgnoreCase("year")) {
//                                    if (column.asBigInteger() == null) {
//                                        preparedStatement.setString(columnIndex + 1, null);
//                                    } else {
//                                        preparedStatement.setInt(columnIndex + 1, column.asBigInteger().intValue());
//                                    }
//                                } else {
//                                    java.sql.Date sqlDate = null;
//                                    try {
//                                        utilDate = column.asDate();
//                                    } catch (DataXException e) {
//                                        throw new SQLException(String.format(
//                                                "Date 类型转换错误：[%s]", column));
//                                    }
//
//                                    if (null != utilDate) {
//                                        sqlDate = new java.sql.Date(utilDate.getTime());
//                                    }
//                                    preparedStatement.setDate(columnIndex + 1, sqlDate);
//                                }
//                                break;
//
//                            case Types.TIME:
//                                java.sql.Time sqlTime = null;
//                                try {
//                                    utilDate = column.asDate();
//                                } catch (DataXException e) {
//                                    throw new SQLException(String.format(
//                                            "Date 类型转换错误：[%s]", column));
//                                }
//
//                                if (null != utilDate) {
//                                    sqlTime = new java.sql.Time(utilDate.getTime());
//                                }
//                                preparedStatement.setTime(columnIndex + 1, sqlTime);
//                                break;
//
//                            case Types.TIMESTAMP:
//                                Timestamp sqlTimestamp = null;
//                                if (column instanceof StringColumn && column.asString() != null) {
//                                    String timeStampStr = column.asString();
//                                    // JAVA TIMESTAMP 类型入参必须是 "2017-07-12 14:39:00.123566" 格式
//                                    String pattern = "^\\d+-\\d+-\\d+ \\d+:\\d+:\\d+.\\d+";
//                                    boolean isMatch = Pattern.matches(pattern, timeStampStr);
//                                    if (isMatch) {
//                                        sqlTimestamp = Timestamp.valueOf(timeStampStr);
//                                        preparedStatement.setTimestamp(columnIndex + 1, sqlTimestamp);
//                                        break;
//                                    }
//                                }
//                                try {
//                                    utilDate = column.asDate();
//                                } catch (DataXException e) {
//                                    throw new SQLException(String.format(
//                                            "Date 类型转换错误：[%s]", column));
//                                }
//
//                                if (null != utilDate) {
//                                    sqlTimestamp = new Timestamp(
//                                            utilDate.getTime());
//                                }
//                                preparedStatement.setTimestamp(columnIndex + 1, sqlTimestamp);
//                                break;
//
//                            case Types.BINARY:
//                            case Types.VARBINARY:
//                            case Types.BLOB:
//                            case Types.LONGVARBINARY:
//                                preparedStatement.setBytes(columnIndex + 1, column
//                                        .asBytes());
//                                break;
//
//                            case Types.BOOLEAN:
//                                preparedStatement.setInt(columnIndex + 1, column.asBigInteger().intValue());
//                                break;
//
//                            // warn: bit(1) -> Types.BIT 可使用setBoolean
//                            // warn: bit(>1) -> Types.VARBINARY 可使用setBytes
//                            case Types.BIT:
//                                if (this.dataBaseType == DataBaseType.MySql) {
//                                    Boolean asBoolean = column.asBoolean();
//                                    if (asBoolean != null) {
//                                        preparedStatement.setBoolean(columnIndex + 1, asBoolean);
//                                    } else {
//                                        preparedStatement.setNull(columnIndex + 1, Types.BIT);
//                                    }
//                                } else {
//                                    preparedStatement.setString(columnIndex + 1, column.asString());
//                                }
//                                break;
//
//                            default:
//                                boolean isHandled = fillPreparedStatementColumnType4CustomType(preparedStatement, cm,
//                                        columnIndex, columnSqltype, column);
//                                if (isHandled) {
//                                    break;
//                                }
//                                throw DataXException
//                                        .asDataXException(
//                                                DBUtilErrorCode.UNSUPPORTED_TYPE,
//                                                String.format(
//                                                        "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%d], 字段Java类型:[%s]. 请修改表中该字段的类型或者不同步该字段.",
//                                                        cm.getName(),
//                                                        type.type,
//                                                        type.typeName));
//                        }
//                        return preparedStatement;
//                    } catch (DataXException e) {
//                        // fix类型转换或者溢出失败时，将具体哪一列打印出来
//                        if (e.getErrorCode() == CommonErrorCode.CONVERT_NOT_SUPPORT ||
//                                e.getErrorCode() == CommonErrorCode.CONVERT_OVER_FLOW) {
//                            throw DataXException
//                                    .asDataXException(
//                                            e.getErrorCode(),
//                                            String.format(
//                                                    "类型转化错误. 字段名:[%s], 字段类型:[%d], 字段Java类型:[%s]. 请修改表中该字段的类型或者不同步该字段.",
//                                                    cm.getName(),
//                                                    type.type,
//                                                    type.typeName));
//                        } else {
//                            throw e;
//                        }
//                    }
//                }

                private Object toJavaArray(Object val) {
                    if (null == val) {
                        return null;
                    } else if (val instanceof JSONArray) {
                        Object[] valArray = ((JSONArray) val).toArray();
                        for (int i = 0; i < valArray.length; i++) {
                            valArray[i] = this.toJavaArray(valArray[i]);
                        }
                        return valArray;
                    } else {
                        return val;
                    }
                }

                boolean fillPreparedStatementColumnType4CustomType(PreparedStatement ps, ColumnMetaData cm,
                                                                   int columnIndex, int columnSqltype,
                                                                   Column column) throws SQLException {
                    switch (columnSqltype) {
                        case Types.OTHER:
                            if (cm.getType().typeName.startsWith("Tuple")) {
                                throw DataXException
                                        .asDataXException(ClickhouseWriterErrorCode.TUPLE_NOT_SUPPORTED_ERROR, ClickhouseWriterErrorCode.TUPLE_NOT_SUPPORTED_ERROR.getDescription());
                            } else {
                                ps.setString(columnIndex + 1, column.asString());
                            }
                            return true;

                        case Types.ARRAY:
                            Connection conn = ps.getConnection();
                            List<Object> values = JSON.parseArray(column.asString(), Object.class);
                            for (int i = 0; i < values.size(); i++) {
                                values.set(i, this.toJavaArray(values.get(i)));
                            }
                            Array array = conn.createArrayOf("String", values.toArray());
                            ps.setArray(columnIndex + 1, array);
                            return true;

                        default:
                            break;
                    }

                    return false;
                }
            };

            this.commonRdbmsWriterSlave.init(this.writerSliceConfig);
        }

        @Override
        public void prepare() {
            this.commonRdbmsWriterSlave.prepare(this.writerSliceConfig);
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            this.commonRdbmsWriterSlave.startWrite(recordReceiver, this.writerSliceConfig, super.getTaskPluginCollector());
        }

        @Override
        public void post() {
            this.commonRdbmsWriterSlave.post(this.writerSliceConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterSlave.destroy(this.writerSliceConfig);
        }
    }

}
