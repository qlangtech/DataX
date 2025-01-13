package com.alibaba.datax.plugin.unstructuredstorage.reader;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-06 14:29
 **/
public class TEXTFormat extends UnstructuredReader {

    private final BufferedReader reader;
    private final String[] header;
    private final char fieldDelimiter;

    private final String[] rowVals;

    private final int colSize;


    public TEXTFormat(BufferedReader reader, boolean skipHeader, int colSize, Character fieldDelimiter) {
        this.reader = reader;
        this.colSize = colSize;
        this.rowVals = new String[colSize];
        this.fieldDelimiter = fieldDelimiter.charValue();
        try {
            // skipHeader: false 说明header中有内容需要进行读取
            this.header = skipHeader ? null : StringUtils.split(reader.readLine(), fieldDelimiter);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (this.header != null) {
            if (colSize != this.header.length) {
                throw new IllegalStateException("colSize:" + colSize + " is not equal header length:" + this.header.length);
            }
        }
    }

    public String[] getHeader() {
        return this.header;
    }

    @Override
    public boolean hasNext() {
        try {
            String line = reader.readLine();
            if (line == null) {
                return false;
            }

            Arrays.fill(rowVals, null);

            int fieldLength = 0;
            int fieldIndex = 0;
            int fieldStartOffsetIndex = 0;
            final int lineLength = line.length();
            for (int i = 0; i < lineLength; i++) {
                if (line.charAt(i) == fieldDelimiter) {
                    if (fieldLength > 0) {
                        rowVals[fieldIndex] = line.substring(fieldStartOffsetIndex, fieldStartOffsetIndex + fieldLength);
                    }
                    fieldStartOffsetIndex = i + 1;
                    fieldIndex++;
                    fieldLength = 0;
                } else {
                    fieldLength++;
                }
            }

            if (fieldStartOffsetIndex < lineLength) {
                rowVals[fieldIndex] = line.substring(fieldStartOffsetIndex, lineLength);
            }

            //   rowVals = line.split(fieldDelimiter);
            return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String[] next() {
        return this.rowVals;
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(reader);
    }
}
