package com.alibaba.datax.plugin.rdbms.reader.util;

import com.alibaba.datax.common.element.Column;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-07-03 11:35
 **/
public abstract class ColumnBiFunction {
    private final Integer colIndex;

    public ColumnBiFunction(Integer colIndex) {
        this.colIndex = colIndex;
    }

    public Integer getColumnIndex() {
        return this.colIndex;
    }

    public abstract Column toColumn(Object val);

    public abstract Object toInternal(Column col);
}
