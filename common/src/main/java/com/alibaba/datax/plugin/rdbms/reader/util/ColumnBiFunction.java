package com.alibaba.datax.plugin.rdbms.reader.util;

import com.alibaba.datax.common.element.Column;
import com.qlangtech.tis.plugin.ds.DataType;

import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-07-03 11:35
 **/
public abstract class ColumnBiFunction {
    private final Integer colIndex;
    private final DataType type;

    public ColumnBiFunction(DataType type, Integer colIndex) {
        this.type = Objects.requireNonNull(type, "param type can not be null");
        this.colIndex = colIndex;
    }

    public DataType getType() {
        return this.type;
    }

    public Integer getColumnIndex() {
        return this.colIndex;
    }

    /**
     * 将简单值装箱封装成Column
     *
     * @param val
     * @return
     */
    public abstract Column toColumn(Object val);

    /**
     * 将Column中的原始值拆箱
     *
     * @param col
     * @return
     */
    public abstract Object toInternal(Column col);
}
