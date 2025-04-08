package com.alibaba.datax.plugin.unstructuredstorage.reader;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.util.DataXCol2Index;
import com.alibaba.datax.plugin.unstructuredstorage.Compress;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;


public class UnstructuredStorageReaderUtil {
    private static final Logger LOG = LoggerFactory
            .getLogger(UnstructuredStorageReaderUtil.class);

    private UnstructuredStorageReaderUtil() {

    }


    /**
     * 不支持转义
     *
     * @return 分隔符分隔后的字符串数，
     */
    public static String[] splitOneLine(String inputLine, String delimiter) {
        String[] splitedResult = StringUtils.split(inputLine, delimiter);
        return splitedResult;
    }


    public static void readFromStream(DataXCol2Index col2Index, InputStream inputStream, Function<InputStream, UnstructuredReader> unstructuredReaderCreator
            , List<ColumnEntry> cols, String context,
                                      Configuration readerSliceConfig, RecordSender recordSender,
                                      TaskPluginCollector taskPluginCollector) {
        if (unstructuredReaderCreator == null) {
            throw new IllegalArgumentException("param unstructuredReaderCreator can not be null");
        }
        try {
            doReadFromStream(col2Index, unstructuredReaderCreator.apply(inputStream), cols, context,
                    readerSliceConfig, recordSender, taskPluginCollector);
        } catch (NullPointerException e) {
            throw DataXException.asDataXException(
                    UnstructuredStorageReaderErrorCode.RUNTIME_EXCEPTION,
                    "运行时错误, 请联系我们", e);
        } finally {
            IOUtils.closeQuietly(inputStream);
        }

    }

    public static void doReadFromStream(DataXCol2Index col2Index, UnstructuredReader freader, List<ColumnEntry> cols, String context,
                                        Configuration readerSliceConfig, RecordSender recordSender,
                                        TaskPluginCollector taskPluginCollector) {
        try {
            String[] parseRows = null;
            while (freader.hasNext()) {
                parseRows = freader.next();
                transportOneRecord(col2Index, recordSender, cols, parseRows, taskPluginCollector);
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    UnstructuredStorageReaderErrorCode.RUNTIME_EXCEPTION,
                    String.format("运行时异常 : %s,[%s]", e.getMessage(), context), e);
        } finally {
            //  csvReader.close();
            IOUtils.closeQuietly(freader);
        }
    }

    public static Record transportOneRecord(DataXCol2Index col2Index, RecordSender recordSender,
                                            List<ColumnEntry> columnConfigs, String[] sourceLine, TaskPluginCollector taskPluginCollector) {
        Record record = recordSender.createRecord(col2Index);
        //  Column columnGenerated = null;
        String val = null;
        Column col = null;
        for (ColumnEntry colMeta : columnConfigs) {
            if (colMeta.isPlaceholder()) {
                continue;
            }
            try {
                val = sourceLine[colMeta.getIndex()];
                col = colMeta.getCType().apply(val);
                record.addColumn(col);
            } catch (Exception e) {
                throw new RuntimeException("col:" + colMeta.getColName() + ",val:'" + val + "'", e);
            } finally {
                val = null;
            }
        }
        recordSender.sendToWriter(record);
        return record;
    }

    public static List<ColumnEntry> getListColumnEntry(
            Configuration configuration, final String path) {
        List<JSONObject> lists = configuration.getList(path, JSONObject.class);
        if (lists == null) {
            return null;
        }
        List<ColumnEntry> result = new ArrayList<ColumnEntry>();
        for (final JSONObject object : lists) {
            result.add(JSON.parseObject(object.toJSONString(),
                    ColumnEntry.class));
        }
        return result;
    }

    public enum Type {
        STRING, LONG, INT, BOOLEAN, DOUBLE, DATE
    }

    /**
     * check parameter:encoding, compress, filedDelimiter
     */
    public static void validateParameter(Configuration readerConfiguration) {

        // encoding check
        validateEncoding(readerConfiguration);

        //only support compress types
        validateCompress(readerConfiguration);

        //fieldDelimiter check
        validateFieldDelimiter(readerConfiguration);

        // column: 1. index type 2.value type 3.when type is Date, may have format
        validateColumn(readerConfiguration);

    }

    public static void validateEncoding(Configuration readerConfiguration) {
        // encoding check
        String encoding = readerConfiguration
                .getString(
                        com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENCODING,
                        com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.DEFAULT_ENCODING);
        try {
            encoding = encoding.trim();
            readerConfiguration.set(Key.ENCODING, encoding);
            Charsets.toCharset(encoding);
        } catch (UnsupportedCharsetException uce) {
            throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.ILLEGAL_VALUE,
                    String.format("不支持您配置的编码格式 : [%s]", encoding), uce);
        } catch (Exception e) {
            throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                    String.format("编码配置异常, 请联系我们: %s", e.getMessage()), e);
        }
    }

    public static void validateCompress(Configuration readerConfiguration) {
        String compress = readerConfiguration
                .getUnnecessaryValue(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COMPRESS, null, null);
        if (StringUtils.isNotBlank(compress)) {
            compress = StringUtils.trim(compress.toLowerCase());

            Compress.parse(compress);
        } else {
            // 用户可能配置的是 compress:"",空字符串,需要将compress设置为null
            compress = null;
        }
        readerConfiguration.set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COMPRESS, compress);

    }

    public static void validateFieldDelimiter(Configuration readerConfiguration) {
        //fieldDelimiter check
        String delimiterInStr = readerConfiguration.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.FIELD_DELIMITER, null);
        String fileFormat = readerConfiguration.getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_FORMAT);


        if (null == delimiterInStr) {
            if (com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.FILE_FORMAT_TEXT.equals(fileFormat)) {

                throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.REQUIRED_VALUE,
                        String.format("您提供配置文件有误，[%s]是必填参数.",
                                com.alibaba.datax.plugin.unstructuredstorage.reader.Key.FIELD_DELIMITER));
            }

        } else if (1 != delimiterInStr.length()) {
            // warn: if have, length must be one
            throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.ILLEGAL_VALUE,
                    String.format("仅仅支持单字符切分, 您配置的切分为 : [%s]", delimiterInStr));
        }
    }

    public static void validateColumn(Configuration readerConfiguration) {

    }

    public static void validateCsvReaderConfig(Configuration readerConfiguration) {
    }

    /**
     * @param @param  regexPath
     * @param @return
     * @return String
     * @throws
     * @Title: getRegexPathParent
     * @Description: 获取正则表达式目录的父目录
     */
    public static String getRegexPathParent(String regexPath) {
        int endMark;
        for (endMark = 0; endMark < regexPath.length(); endMark++) {
            if ('*' != regexPath.charAt(endMark) && '?' != regexPath.charAt(endMark)) {
                continue;
            } else {
                break;
            }
        }
        int lastDirSeparator = regexPath.substring(0, endMark).lastIndexOf(IOUtils.DIR_SEPARATOR);
        String parentPath = regexPath.substring(0, lastDirSeparator + 1);

        return parentPath;
    }

    /**
     * @param @param  regexPath
     * @param @return
     * @return String
     * @throws
     * @Title: getRegexPathParentPath
     * @Description: 获取含有通配符路径的父目录，目前只支持在最后一级目录使用通配符*或者?.
     * (API jcraft.jsch.ChannelSftp.ls(String path)函数限制)  http://epaul.github.io/jsch-documentation/javadoc/
     */
    public static String getRegexPathParentPath(String regexPath) {
        int lastDirSeparator = regexPath.lastIndexOf(IOUtils.DIR_SEPARATOR);
        String parentPath = "";
        parentPath = regexPath.substring(0, lastDirSeparator + 1);
        if (parentPath.contains("*") || parentPath.contains("?")) {
            throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.ILLEGAL_VALUE,
                    String.format("配置项目path中：[%s]不合法，目前只支持在最后一级目录使用通配符*或者?", regexPath));
        }
        return parentPath;
    }


}
