package com.alibaba.datax.plugin.unstructuredstorage.writer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.Record;

import java.io.IOException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-06 13:37
 **/
public abstract class BasicPainWriter implements UnstructuredWriter {
    protected final DateFormat dateParse;
    protected final String nullFormat;

    public BasicPainWriter(DateFormat dateParse, String nullFormat) {
        this.dateParse = dateParse;
        this.nullFormat = nullFormat;
    }

    @Override
    public void writeHeader(List<String> headers) throws IOException {
        this.writeOneRecord(headers);
    }

    @Override
    public final void writeOneRecord(Record record) throws IOException {
        List<String> splitedRows = new ArrayList<String>();
        int recordLength = record.getColumnNumber();
        if (0 != recordLength) {
            Column column;
            for (int i = 0; i < recordLength; i++) {
                column = record.getColumn(i);
                if (null != column.getRawData()) {
                    boolean isDateColumn = column instanceof DateColumn;
                    if (!isDateColumn) {
                        splitedRows.add(column.asString());
                    } else {
                        if (null != dateParse) {
                            splitedRows.add(dateParse.format(column
                                    .asDate()));
                        } else {
                            splitedRows.add(column.asString());
                        }
                    }
                } else {
                    // warn: it's all ok if nullFormat is null
                    splitedRows.add(nullFormat);
                }
            }
        }
        this.writeOneRecord(splitedRows);
    }

    public abstract void writeOneRecord(List<String> splitedRows) throws IOException;


}
