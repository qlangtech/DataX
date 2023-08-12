package com.alibaba.datax.plugin.unstructuredstorage.writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.text.DateFormat;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-06 13:21
 **/
public abstract class TextWriterImpl extends BasicPainWriter {
    private static final Logger LOG = LoggerFactory.getLogger(TextWriterImpl.class);
    // text StringUtils的join方式, 简单的字符串拼接
    protected char fieldDelimiter;
    protected Writer textWriter;

    public TextWriterImpl(Writer writer, DateFormat dateParse, String nullFormat, char fieldDelimiter) {
        super(dateParse, nullFormat);
        this.fieldDelimiter = fieldDelimiter;
        this.textWriter = writer;
    }

//    @Override
//    public void writeOneRecord(Record row) throws IOException {
//        if (splitedRows.isEmpty()) {
//            LOG.info("Found one record line which is empty.");
//        }
//        this.textWriter.write(String.format("%s%s",
//                StringUtils.join(splitedRows, this.fieldDelimiter),
//                IOUtils.LINE_SEPARATOR));
    //  }

    @Override
    public void flush() throws IOException {
        this.textWriter.flush();
    }

    @Override
    public void close() throws IOException {
        this.textWriter.close();
    }

}
