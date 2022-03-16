/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.tis.plugin.ds;


import com.alibaba.datax.plugin.writer.hdfswriter.SupportHiveDataType;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.sql.Types;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-02-19 08:50
 **/
public class DataType implements Serializable {

    public final int type;
    public final int columnSize;
    // decimal 的小数位长度
    private Integer decimalDigits;

    public DataType(int type) {
        this(type, -1);
    }

    public DataXReaderColType getCollapse() {
        switch (this.type) {
            case Types.INTEGER:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.BIGINT:
                return DataXReaderColType.Long;
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.DECIMAL:
                return DataXReaderColType.Double;
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                return DataXReaderColType.Date;
            case Types.BIT:
            case Types.BOOLEAN:
                return DataXReaderColType.Boolean;
            case Types.BLOB:
            case Types.BINARY:
            case Types.LONGVARBINARY:
            case Types.VARBINARY:
                return DataXReaderColType.Bytes;
            default:
                return DataXReaderColType.STRING;
        }
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        switch (this.type) {
            case Types.INTEGER:
                return visitor.intType(this);
            case Types.TINYINT:
                return visitor.tinyIntType(this);
            case Types.SMALLINT:
                return visitor.smallIntType(this);
            case Types.BIGINT:
                return visitor.longType(this);
            case Types.FLOAT:
                return visitor.floatType(this);
            case Types.DOUBLE:
                return visitor.doubleType(this);
            case Types.DECIMAL:
                return visitor.decimalType(this);
            case Types.DATE:
                return visitor.dateType(this);
            case Types.TIME:
                return visitor.timeType(this);
            case Types.TIMESTAMP:
                return visitor.timestampType(this);
            case Types.BIT:
            case Types.BOOLEAN:
                return visitor.bitType(this);
            case Types.BLOB:
            case Types.BINARY:
            case Types.LONGVARBINARY:
            case Types.VARBINARY:
                return visitor.blobType(this);
            case Types.VARCHAR:
            case Types.LONGNVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
                // return visitor.varcharType(this);
            default:
                return visitor.varcharType(this);// "VARCHAR(" + type.columnSize + ")";
        }
    }

    public static SupportHiveDataType convert2HiveType(DataType type) {
        Objects.requireNonNull(type, "param type can not be null");
        return type.accept(new TypeVisitor<SupportHiveDataType>() {
            @Override
            public SupportHiveDataType intType(DataType type) {
                return SupportHiveDataType.INT;
            }

            @Override
            public SupportHiveDataType floatType(DataType type) {
                return SupportHiveDataType.FLOAT;
            }

            @Override
            public SupportHiveDataType decimalType(DataType type) {
                return SupportHiveDataType.DOUBLE;
            }

            @Override
            public SupportHiveDataType timeType(DataType type) {
                return SupportHiveDataType.TIMESTAMP;
            }

            @Override
            public SupportHiveDataType dateType(DataType type) {
                return SupportHiveDataType.DATE;
            }

            @Override
            public SupportHiveDataType timestampType(DataType type) {
                return SupportHiveDataType.TIMESTAMP;
            }

            @Override
            public SupportHiveDataType tinyIntType(DataType dataType) {
                return SupportHiveDataType.TINYINT;
            }

            @Override
            public SupportHiveDataType smallIntType(DataType dataType) {
                return SupportHiveDataType.SMALLINT;
            }

            @Override
            public SupportHiveDataType longType(DataType type) {
                return SupportHiveDataType.BIGINT;
            }

            @Override
            public SupportHiveDataType doubleType(DataType type) {
                return SupportHiveDataType.DOUBLE;
            }


            @Override
            public SupportHiveDataType bitType(DataType type) {
                return SupportHiveDataType.BOOLEAN;
            }

            @Override
            public SupportHiveDataType blobType(DataType type) {
                return SupportHiveDataType.STRING;
            }

            @Override
            public SupportHiveDataType varcharType(DataType type) {
                return SupportHiveDataType.STRING;
            }
        });
    }

    public Integer getDecimalDigits() {
        return decimalDigits;
    }

    public void setDecimalDigits(Integer decimalDigits) {
        this.decimalDigits = decimalDigits;
    }

    /**
     * @param type       java.sql.Types
     * @param columnSize
     */
    public DataType(int type, int columnSize) {
        this.type = type;
        this.columnSize = columnSize;
    }

    public String getS() {
        return this.type + "," + this.columnSize
                + "," + (this.decimalDigits != null ? this.decimalDigits : StringUtils.EMPTY);
    }

    /**
     * 反序列化
     *
     * @param ser
     * @return
     */
    public static DataType ds(String ser) {
        Pattern p = Pattern.compile("(-?\\d+),(-?\\d+),(\\d*)");
        Matcher matcher = p.matcher(ser);
        if (!matcher.matches()) {
            throw new IllegalStateException("val is illegal:" + ser);
        }
        DataType type = new DataType(Integer.parseInt(matcher.group(1)), Integer.parseInt(matcher.group(2)));
        String d = matcher.group(3);
        if (StringUtils.isNotEmpty(d)) {
            type.decimalDigits = Integer.parseInt(d);
        }
        return type;
    }

    @Override
    public String toString() {
        return "{" +
                "type=" + type +
                ", columnSize=" + columnSize +
                ", decimalDigits=" + decimalDigits +
                '}';
    }

    public interface TypeVisitor<T> {
        default T intType(DataType type) {
            return longType(type);
        }

        T longType(DataType type);

        default T floatType(DataType type) {
            return doubleType(type);
        }

        T doubleType(DataType type);

        default T decimalType(DataType type) {
            return doubleType(type);
        }

        T dateType(DataType type);

        default T timeType(DataType type) {
            return timestampType(type);
        }

        T timestampType(DataType type);

        T bitType(DataType type);

        T blobType(DataType type);

        T varcharType(DataType type);

        default T tinyIntType(DataType dataType) {
            return intType(dataType);
        }

        default T smallIntType(DataType dataType) {
            return intType(dataType);
        }
    }
}
