package com.alibaba.datax.plugin.unstructuredstorage.writer;

import com.csvreader.CsvWriter;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.text.DateFormat;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-06 13:22
 **/
public abstract class CsvWriterImpl extends BasicPainWriter {
    private static final Logger LOG = LoggerFactory.getLogger(CsvWriterImpl.class);
    // csv 严格符合csv语法, 有标准的转义等处理
    protected char fieldDelimiter;
    protected CsvWriter csvWriter;

    public CsvWriterImpl(Writer writer, DateFormat dateParse, String nullFormat, char fieldDelimiter) {
        super(dateParse, nullFormat);
        this.fieldDelimiter = fieldDelimiter;
        this.csvWriter = new CsvWriter(writer, this.fieldDelimiter);
        this.csvWriter.setTextQualifier('"');
        this.csvWriter.setUseTextQualifier(true);
        // warn: in linux is \n , in windows is \r\n
        this.csvWriter.setRecordDelimiter(IOUtils.LINE_SEPARATOR.charAt(0));
    }

//    @Override
//    public void writeOneRecord(Record splitedRows) throws IOException {
//        if (splitedRows.isEmpty()) {
//            LOG.info("Found one record line which is empty.");
//        }
//        this.csvWriter.writeRecord((String[]) splitedRows
//                .toArray(new String[0]));
    //  }

    @Override
    public void flush() throws IOException {
        this.csvWriter.flush();
    }

    @Override
    public void close() throws IOException {
        this.csvWriter.close();
    }

}
