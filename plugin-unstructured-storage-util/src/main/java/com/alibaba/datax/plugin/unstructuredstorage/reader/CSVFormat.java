package com.alibaba.datax.plugin.unstructuredstorage.reader;

import com.csvreader.CsvReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-06 14:28
 **/
public abstract class CSVFormat extends UnstructuredReader {
    private static final Logger LOG = LoggerFactory.getLogger(CSVFormat.class);
    private final CsvReader csvReader;


    public CSVFormat(BufferedReader reader) {
        this.csvReader = new CsvReader(reader);
        //csvReader.setDelimiter(fieldDelimiter);
        this.setCsvReaderConfig(this.csvReader);
    }

    protected void setCsvReaderConfig(CsvReader csvReader) {
        throw new UnsupportedOperationException();
//        if (null != UnstructuredStorageReaderUtil.csvReaderConfigMap && !UnstructuredStorageReaderUtil.csvReaderConfigMap.isEmpty()) {
//            try {
//                BeanUtils.populate(csvReader, UnstructuredStorageReaderUtil.csvReaderConfigMap);
//                LOG.info(String.format("csvReaderConfig设置成功,设置后CsvReader:%s", JSON.toJSONString(csvReader)));
//            } catch (Exception e) {
//                LOG.info(String.format("WARN!!!!忽略csvReaderConfig配置!通过BeanUtils.populate配置您的csvReaderConfig发生异常,您配置的值为: %s;请检查您的配置!CsvReader使用默认值[%s]",
//                        JSON.toJSONString(UnstructuredStorageReaderUtil.csvReaderConfigMap), JSON.toJSONString(csvReader)));
//            }
//        } else {
//            //默认关闭安全模式, 放开10W字节的限制
//            csvReader.setSafetySwitch(false);
//            LOG.info(String.format("CsvReader使用默认值[%s],csvReaderConfig值为[%s]", JSON.toJSONString(csvReader), JSON.toJSONString(UnstructuredStorageReaderUtil.csvReaderConfigMap)));
//        }
    }


    @Override
    public boolean hasNext() throws IOException {
        return this.csvReader.readRecord();
    }

    @Override
    public String[] next() throws IOException {
        String[] vals = this.csvReader.getValues();
        return vals;
    }

    @Override
    public void close() throws IOException {
        this.csvReader.close();
    }
}
