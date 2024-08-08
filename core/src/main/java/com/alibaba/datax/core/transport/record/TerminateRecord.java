package com.alibaba.datax.core.transport.record;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.ICol2Index;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.rdbms.reader.util.DataXCol2Index;

import java.util.Map;

/**
 * 作为标示 生产者已经完成生产的标志
 */
public class TerminateRecord implements Record {
    private final static TerminateRecord SINGLE = new TerminateRecord();

    private TerminateRecord() {
    }

    @Override
    public void setCol2Index(ICol2Index mapper) {

    }

    @Override
    public String getString(String field, boolean origin) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ICol2Index getCol2Index() {
        throw new UnsupportedOperationException();
    }

    public static TerminateRecord get() {
        return SINGLE;
    }

    @Override
    public void addColumn(Column column) {
    }

    @Override
    public void setString(String field, String val) {

    }

    @Override
    public void setColumn(String field, Object column) {

    }

    @Override
    public Object getColumn(String field) {
        return null;
    }

    @Override
    public Column getColumn(int i) {
        return null;
    }

    @Override
    public int getColumnNumber() {
        return 0;
    }

    @Override
    public int getByteSize() {
        return 0;
    }

    @Override
    public int getMemorySize() {
        return 0;
    }

    @Override
    public void setColumn(int i, Column column) {
        return;
    }
}
