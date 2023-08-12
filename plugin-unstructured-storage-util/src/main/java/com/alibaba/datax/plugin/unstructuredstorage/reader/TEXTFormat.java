package com.alibaba.datax.plugin.unstructuredstorage.reader;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-06 14:29
 **/
public class TEXTFormat extends UnstructuredReader {

    private final BufferedReader reader;
    private final String[] header;
    private final String fieldDelimiter;

    private String[] rowVals = null;


    public TEXTFormat(BufferedReader reader, boolean skipHeader, Character fieldDelimiter) {
        this.reader = reader;
        this.fieldDelimiter = String.valueOf(fieldDelimiter.charValue());
        try {
            // skipHeader: false 说明header中有内容需要进行读取
            this.header = skipHeader ? null : StringUtils.split(reader.readLine(), fieldDelimiter);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String[] getHeader() {
        return this.header;
    }

    @Override
    public boolean hasNext() throws IOException {
        String line = reader.readLine();
        if (line == null) {
            return false;
        }
        rowVals = line.split(fieldDelimiter);
        return true;
    }

    @Override
    public String[] next() throws IOException {
        return this.rowVals;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(reader);
    }
}
