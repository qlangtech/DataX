package com.alibaba.datax.core.statistics.plugin.task.util;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DirtyRecordCreator;
import com.alibaba.datax.common.element.ICol2Index;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.fastjson.JSON;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-10-13 09:49
 **/
public class DirtyRecord implements Record {
    private static final ErrorCode RUNTIME_ERROR = new ErrorCode() {
        @Override
        public String getCode() {
            return "dirtyRecordError";
        }

        @Override
        public String getDescription() {
            return "dirtyRecordError";
        }
    };

    private List<Column> columns = new ArrayList<Column>();

    public static DirtyRecordCreator create(Record record) {
        if (record == null) {
            throw new IllegalArgumentException("param record can not be null");
        }
        return new DirtyRecordCreator() {
            @Override
            public Object createDirtyRecordDescriptor() {
                return DirtyRecord.asDirtyRecord(record).getColumns();
            }

            @Override
            public int getByteSize() {
                return record.getByteSize();
            }

            @Override
            public int getMemorySize() {
                return record.getMemorySize();
            }
        };
    }


    public static DirtyRecord asDirtyRecord(final Record record) {
        DirtyRecord result = new DirtyRecord();
        for (int i = 0; i < record.getColumnNumber(); i++) {
            result.addColumn(record.getColumn(i));
        }

        return result;
    }

    @Override
    public String getString(String field, boolean origin) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCol2Index(ICol2Index mapper) {
        throw DataXException.asDataXException(RUNTIME_ERROR,
                "该方法不支持!");
    }

    @Override
    public ICol2Index getCol2Index() {
        throw DataXException.asDataXException(RUNTIME_ERROR,
                "该方法不支持!");
    }

    @Override
    public void addColumn(Column column) {
        this.columns.add(
                DirtyColumn.asDirtyColumn(column, this.columns.size()));
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this.columns);
    }

    @Override
    public void setColumn(int i, Column column) {
        throw DataXException.asDataXException(RUNTIME_ERROR,
                "该方法不支持!");
    }

    @Override
    public void setString(String field, String val) {
        throw DataXException.asDataXException(RUNTIME_ERROR,
                "该方法不支持!");
    }

    @Override
    public void setColumn(String field, Object column) {
        throw DataXException.asDataXException(RUNTIME_ERROR,
                "该方法不支持!");
    }

    @Override
    public Column getColumn(String field) {
        throw DataXException.asDataXException(RUNTIME_ERROR,
                "该方法不支持!");
    }

    @Override
    public Column getColumn(int i) {
        throw DataXException.asDataXException(RUNTIME_ERROR,
                "该方法不支持!");
    }

    @Override
    public int getColumnNumber() {
        throw DataXException.asDataXException(RUNTIME_ERROR,
                "该方法不支持!");
    }

    @Override
    public int getByteSize() {
        throw DataXException.asDataXException(RUNTIME_ERROR,
                "该方法不支持!");
    }

    @Override
    public int getMemorySize() {
        throw DataXException.asDataXException(RUNTIME_ERROR,
                "该方法不支持!");
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }


    static class DirtyColumn extends Column {
        private int index;

        public static Column asDirtyColumn(final Column column, int index) {
            return new DirtyColumn(column, index);
        }

        private DirtyColumn(Column column, int index) {
            this(null == column ? null : column.getRawData(),
                    null == column ? Type.NULL : column.getType(),
                    null == column ? 0 : column.getByteSize(), index);
        }

        public int getIndex() {
            return index;
        }

        public void setIndex(int index) {
            this.index = index;
        }

        @Override
        public Long asLong() {
            throw DataXException.asDataXException(RUNTIME_ERROR,
                    "该方法不支持!");
        }

        @Override
        public Double asDouble() {
            throw DataXException.asDataXException(RUNTIME_ERROR,
                    "该方法不支持!");
        }

        @Override
        public String asString() {
            throw DataXException.asDataXException(RUNTIME_ERROR,
                    "该方法不支持!");
        }

        @Override
        public Date asDate() {
            throw DataXException.asDataXException(RUNTIME_ERROR,
                    "该方法不支持!");
        }

        @Override
        public Date asDate(String dateFormat) {
            throw DataXException.asDataXException(RUNTIME_ERROR,
                    "该方法不支持!");
        }

        @Override
        public byte[] asBytes() {
            throw DataXException.asDataXException(RUNTIME_ERROR,
                    "该方法不支持!");
        }

        @Override
        public Boolean asBoolean() {
            throw DataXException.asDataXException(RUNTIME_ERROR,
                    "该方法不支持!");
        }

        @Override
        public BigDecimal asBigDecimal() {
            throw DataXException.asDataXException(RUNTIME_ERROR,
                    "该方法不支持!");
        }

        @Override
        public BigInteger asBigInteger() {
            throw DataXException.asDataXException(RUNTIME_ERROR,
                    "该方法不支持!");
        }

        private DirtyColumn(Object object, Type type, int byteSize, int index) {
            super(object, type, byteSize);
            this.setIndex(index);
        }


    }
}
