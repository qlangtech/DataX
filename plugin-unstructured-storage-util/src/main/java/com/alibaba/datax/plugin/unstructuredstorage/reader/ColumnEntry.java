package com.alibaba.datax.plugin.unstructuredstorage.reader;

import com.alibaba.datax.common.element.Column;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.function.Function;

public class ColumnEntry {
    private Integer index;
    private String type;
    private String value;
    private String format;
    private DateFormat dateParse;
    private Function<String, Column> colValSetter;
    private String colName;

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    public Integer getIndex() {
        return index;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

    public Function<String, Column> getCType() {
        return colValSetter;
    }

    public void setType(String type, Function<String, Column> colValSetter) {
        this.type = type;
        this.colValSetter = colValSetter; //UnstructuredStorageReaderUtil.Type.valueOf(type.toUpperCase());
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
        if (StringUtils.isNotBlank(this.format)) {
            this.dateParse = new SimpleDateFormat(this.format);
        }
    }

    public DateFormat getDateFormat() {
        return this.dateParse;
    }

    public String toJSONString() {
        return ColumnEntry.toJSONString(this);
    }

    public static String toJSONString(ColumnEntry columnEntry) {
        return JSON.toJSONString(columnEntry);
    }
}
