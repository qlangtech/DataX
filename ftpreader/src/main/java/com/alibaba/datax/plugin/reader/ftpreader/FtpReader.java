package com.alibaba.datax.plugin.reader.ftpreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.ftp.common.FtpHelper;
import com.alibaba.datax.plugin.rdbms.reader.util.DataXCol2Index;
import com.alibaba.datax.plugin.unstructuredstorage.reader.ColumnEntry;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredReader;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugin.ds.JDBCTypes;
import com.qlangtech.tis.plugin.ds.RunningContext;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FtpReader extends Reader {

    protected static List<String> getPaths(Configuration cfg) {
        List<String> path = null;
        Object pathObj = cfg.get(Key.PATH);
        if (pathObj == null) {
            throw new NullPointerException("property of key:" + Key.PATH + " relevant val can not be null");
        }
        if (pathObj instanceof List) {
            path = cfg.getList(Key.PATH, String.class);
        } else if (pathObj instanceof String) {
            path = Collections.singletonList((String) pathObj);
        } else {
            throw new IllegalStateException("illegal type of pathObj:" + pathObj.getClass());
        }
        if (CollectionUtils.isEmpty(path)) {
            throw DataXException.asDataXException(FtpReaderErrorCode.REQUIRED_VALUE, "您需要指定待读取的源目录或文件");
        }
        return path;
    }

    private static Optional<String> getEntityName(Configuration cfg) {
        return Optional.ofNullable(cfg.getString(Key.ENTITY_NAME));
    }

    public static abstract class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originConfig = null;

        protected List<String> path = null;

        private Set<ITDFSSession.Res> sourceFiles;

        private ITDFSSession dfsSession = null;

        @Override
        public void init() {
            this.originConfig = this.getPluginJobConf();
            this.sourceFiles = new HashSet<ITDFSSession.Res>();

            this.validateParameter();
            UnstructuredStorageReaderUtil.validateParameter(this.originConfig);
            dfsSession = createTdfsSession();
            this.path = this.getReaderPaths(this.originConfig, this.dfsSession, getEntityName(this.originConfig));
        }

        protected abstract ITDFSSession createTdfsSession();


        private void validateParameter() {
        }

        protected List<String> getReaderPaths(Configuration cfg, ITDFSSession dfsSession, Optional<String> entityName) {
            return getPaths(cfg);
        }

        @Override
        public void prepare() {
            LOG.debug("prepare() begin...");
            if (CollectionUtils.isEmpty(this.path)) {
                throw new IllegalStateException("property path can not be empty");
            }
            this.sourceFiles = this.findAllMatchedRes(this.path, dfsSession); //dfsSession.getAllFiles(path, 0, maxTraversalLevel);

            LOG.info(String.format("您即将读取的文件数为: [%s]", this.sourceFiles.size()));
        }

        protected abstract Set<ITDFSSession.Res> findAllMatchedRes(List<String> targetPath, ITDFSSession dfsSession);

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
            try {
                this.dfsSession.close();
            } catch (Exception e) {
                String message = String.format("关闭与ftp服务器连接失败: [%s]", e.getMessage());
                LOG.error(message, e);
            }
        }

        // warn: 如果源目录为空会报错，拖空目录意图=>空文件显示指定此意图
        @Override
        public List<Configuration> split(int adviceNumber) {
            LOG.debug("split() begin...");
            List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();

            // warn:每个slice拖且仅拖一个文件,
            // int splitNumber = adviceNumber;
            int splitNumber = this.sourceFiles.size();
            if (0 == splitNumber) {
                throw DataXException.asDataXException(FtpReaderErrorCode.EMPTY_DIR_EXCEPTION, String.format("未能找到待读取的文件,请确认您的配置项path: %s", this.originConfig.getString(Key.PATH)));
            }

            List<List<ITDFSSession.Res>> splitedSourceFiles = this.splitSourceFiles(new ArrayList<ITDFSSession.Res>(this.sourceFiles), splitNumber);
            List<String> sourceFiles = null;
            for (List<ITDFSSession.Res> files : splitedSourceFiles) {
                Configuration splitedConfig = this.originConfig.clone();

                sourceFiles = files.stream().map((r) -> r.fullPath)
                        .filter((f) -> !StringUtils.endsWith(f, FtpHelper.KEY_META_FILE)).collect(Collectors.toList());
                if (CollectionUtils.isEmpty(sourceFiles)) {
                    throw new IllegalStateException("sourceFiles can not be empty,raw sourceFiles:"
                            + this.sourceFiles.stream().map((ff) -> ff.fullPath).collect(Collectors.joining(",")));
                }
                splitedConfig.set(Constant.SOURCE_FILES, sourceFiles);
                readerSplitConfigs.add(splitedConfig);
            }
            LOG.debug("split() ok and end...");
            return readerSplitConfigs;
        }

        private <T> List<List<T>> splitSourceFiles(final List<T> sourceList, int adviceNumber) {
            List<List<T>> splitedList = new ArrayList<List<T>>();
            int averageLength = sourceList.size() / adviceNumber;
            averageLength = averageLength == 0 ? 1 : averageLength;

            for (int begin = 0, end = 0; begin < sourceList.size(); begin = end) {
                end = begin + averageLength;
                if (end > sourceList.size()) {
                    end = sourceList.size();
                }
                splitedList.add(sourceList.subList(begin, end));
            }
            return splitedList;
        }

    }

    public static abstract class Task extends Reader.Task {
        private static Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration readerSliceConfig;
        private List<String> sourceFiles;

        protected ITDFSSession hdfsSession = null;
        private List<ColumnEntry> colsMeta = null;
        protected Function<InputStream, UnstructuredReader> unstructuredReaderCreator;


        @Override
        public void init() {//连接重试
            /* for ftp connection */
            this.readerSliceConfig = this.getPluginJobConf();
//            this.host = readerSliceConfig.getString(Key.HOST);
//            this.protocol = readerSliceConfig.getString(Key.PROTOCOL);
//            this.username = readerSliceConfig.getString(Key.USERNAME);
//            this.password = readerSliceConfig.getString(Key.PASSWORD);
//            this.timeout = readerSliceConfig.getInt(Key.TIMEOUT, Constant.DEFAULT_TIMEOUT);

            this.sourceFiles = this.readerSliceConfig.getList(Constant.SOURCE_FILES, String.class);
            // this.port = readerSliceConfig.getInt(Key.PORT, Constant.DEFAULT_SFTP_PORT);
            this.hdfsSession = createTdfsSession();// FtpHelper.createFtpClient(readerSliceConfig);
            this.colsMeta = createColsMeta(getEntityName(this.readerSliceConfig));


            Objects.requireNonNull(unstructuredReaderCreator, "unstructuredReaderCreator can not be null");
//            if ("sftp".equals(protocol)) {
//                //sftp协议
//                this.port = readerSliceConfig.getInt(Key.PORT, Constant.DEFAULT_SFTP_PORT);
//                this.ftpHelper = new SftpHelper();
//            } else if ("ftp".equals(protocol)) {
//                // ftp 协议
//                this.port = readerSliceConfig.getInt(Key.PORT, Constant.DEFAULT_FTP_PORT);
//                this.connectPattern = readerSliceConfig.getString(Key.CONNECTPATTERN, Constant.DEFAULT_FTP_CONNECT_PATTERN);// 默认为被动模式
//                this.ftpHelper = new StandardFtpHelper();
//            }
//            ftpHelper.loginFtpServer(host, username, password, port, timeout, connectPattern);
        }


        protected abstract List<ColumnEntry> createColsMeta(Optional<String> entityName);

        protected abstract ITDFSSession createTdfsSession();

        @Override
        public void prepare() {

        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {
            try {
                this.hdfsSession.close();
            } catch (Exception e) {
                String message = String.format("关闭与ftp服务器连接失败: [%s] ", e.getMessage());
                LOG.error(message, e);
            }
        }

        @Override
        public void startRead(RecordSender recordSender) {
            LOG.debug("start read source files...");
            DataXCol2Index col2Idx = DataXCol2Index.getCol2Index(Optional.empty(), new RunningContext() {
            }, colsMeta.stream().map((col) -> IColMetaGetter.create(col.getColName(), DataType.getType(JDBCTypes.VARCHAR))).collect(Collectors.toUnmodifiableList()));

            for (String fileName : this.sourceFiles) {
                LOG.info(String.format("reading file : [%s]", fileName));

                transformReocrds2Sender(col2Idx, recordSender, fileName);

                recordSender.flush();
            }

            LOG.debug("end read source files...");
        }

        /**
         * @param recordSender
         * @param fileName     源文件
         */
        protected void transformReocrds2Sender(DataXCol2Index col2Idx, RecordSender recordSender, String fileName) {
            InputStream inputStream = hdfsSession.getInputStream(fileName);

            UnstructuredStorageReaderUtil.readFromStream(col2Idx, inputStream
                    , this.unstructuredReaderCreator
                    , Objects.requireNonNull(colsMeta, "colsMeta can not be null")
                    , fileName
                    , this.readerSliceConfig
                    , recordSender, this.getTaskPluginCollector());
        }

    }
}
