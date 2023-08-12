package com.alibaba.datax.plugin.unstructuredstorage.reader;

//import com.alibaba.datax.common.util.Configuration;
//import com.alibaba.datax.plugin.unstructuredstorage.writer.Constant;
//import java.io.BufferedReader;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-06 14:30
 **/
public class UnstructuredReaderUtils {
//    public static UnstructuredReader create(Configuration cfg, BufferedReader reader) {
//        String fileFormat = cfg.getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_FORMAT);
//
//        if (com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.FILE_FORMAT_TEXT.equals(fileFormat)) {
////BufferedReader reader, boolean skipHeader, String fieldDelimiter
//            //  com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.DEFAULT_SKIP_HEADER
//            Boolean skipHeader = cfg.getBool(Key.SKIP_HEADER);
//            if (skipHeader == null) {
//                throw new IllegalArgumentException("param " + Key.SKIP_HEADER + " can not be null");
//            }
//            //com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.DEFAULT_FIELD_DELIMITER
//            Character fieldDelimiter = cfg.getChar(Key.FIELD_DELIMITER);
//            if (fieldDelimiter == null) {
//                throw new IllegalArgumentException("param " + Key.FIELD_DELIMITER + " can not be null");
//            }
//            TEXTFormat textFormat = new TEXTFormat(reader, skipHeader, fieldDelimiter);
//            return textFormat;
//        } else if (Constant.FILE_FORMAT_CSV.equals(fileFormat)) {
//            return new CSVFormat(reader);
//        } else {
//            throw new IllegalArgumentException("illegal fileFormat:" + fileFormat);
//        }
//    }
}
