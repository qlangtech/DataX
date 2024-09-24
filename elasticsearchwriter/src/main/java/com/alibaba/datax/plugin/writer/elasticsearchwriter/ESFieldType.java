package com.alibaba.datax.plugin.writer.elasticsearchwriter;

import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.JDBCTypes;
import org.apache.commons.lang3.StringUtils;

/**
 * https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html
 * <p>
 * Created by xiongfeng.bxf on 17/3/1.
 */
public enum ESFieldType {
   // ID(DataType.createVarChar(100)), // STRING,
    TEXT(DataType.getType(JDBCTypes.LONGVARCHAR)) //
    , KEYWORD(DataType.createVarChar(100)), //
    LONG(DataType.getType(JDBCTypes.BIGINT))   //
    , INTEGER(DataType.getType(JDBCTypes.INTEGER)), //
    SHORT(DataType.getType(JDBCTypes.SMALLINT)) //
    , BYTE(DataType.getType(JDBCTypes.BINARY)), //
    DOUBLE(DataType.getType(JDBCTypes.DOUBLE)),  //
    FLOAT(DataType.getType(JDBCTypes.FLOAT)), //
    DATE(DataType.getType(JDBCTypes.TIMESTAMP)), //
    BOOLEAN(DataType.getType(JDBCTypes.BOOLEAN)), //
    BINARY(DataType.getType(JDBCTypes.BINARY)), //
    INTEGER_RANGE(DataType.getType(JDBCTypes.INTEGER)), //
    FLOAT_RANGE(DataType.getType(JDBCTypes.FLOAT)), LONG_RANGE((DataType.getType(JDBCTypes.BIGINT))),
    DOUBLE_RANGE(DataType.getType(JDBCTypes.DOUBLE)), DATE_RANGE(DataType.getType(JDBCTypes.DATE)),
    GEO_POINT(DataType.createVarChar(200)), GEO_SHAPE(DataType.createVarChar(200)),

    IP(DataType.createVarChar(100)), COMPLETION((DataType.createVarChar(200))),
    TOKEN_COUNT(DataType.createVarChar(200)),

    ARRAY(DataType.createVarChar(1000)), OBJECT(DataType.createVarChar(1000)), NESTED(DataType.createVarChar(2000));
    private final DataType type;

    private ESFieldType(DataType type) {
        this.type = type;
    }

    public static ESFieldType getESFieldType(String type) {
        if (StringUtils.isEmpty(type)) {
            throw new IllegalArgumentException("illegal param type, can not be null");
        }
        for (ESFieldType f : ESFieldType.values()) {
            if (f.name().compareTo(type.toUpperCase()) == 0) {
                return f;
            }
        }
        throw new IllegalStateException("type:" + type + " is illegal");
    }

    public DataType getDataType() {
        return this.type;
    }
}
