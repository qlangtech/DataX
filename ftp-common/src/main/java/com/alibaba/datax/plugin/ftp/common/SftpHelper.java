package com.alibaba.datax.plugin.ftp.common;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.reader.ftpreader.FtpReaderErrorCode;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import com.alibaba.datax.plugin.writer.ftpwriter.FtpWriterErrorCode;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.jcraft.jsch.*;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.qlangtech.tis.plugin.tdfs.TDFSLinker;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

public class SftpHelper extends FtpHelper {
    private static final Logger LOG = LoggerFactory.getLogger(SftpHelper.class);

    Session session = null;
    ChannelSftp channelSftp = null;

    public SftpHelper(TDFSLinker dfsLinker) {
        super(dfsLinker);
    }

    @Override
    public void loginFtpServer(String host, String username, String password, int port, int timeout,
                               String connectMode) {
        JSch jsch = new JSch(); // 创建JSch对象
        try {
            session = jsch.getSession(username, host, port);
            // 根据用户名，主机ip，端口获取一个Session对象
            // 如果服务器连接不上，则抛出异常
            if (session == null) {
                throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_LOGIN,
                        "session is null,无法通过sftp与服务器建立链接，请检查主机名和用户名是否正确.");
            }

            session.setPassword(password); // 设置密码
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config); // 为Session对象设置properties
            session.setTimeout(timeout); // 设置timeout时间
            session.connect(); // 通过Session建立链接

            channelSftp = (ChannelSftp) session.openChannel("sftp"); // 打开SFTP通道
            channelSftp.connect(); // 建立SFTP通道的连接

            //设置命令传输编码
            //String fileEncoding = System.getProperty("file.encoding");
            //channelSftp.setFilenameEncoding(fileEncoding);
        } catch (JSchException e) {
            if (null != e.getCause()) {
                String cause = e.getCause().toString();
                String unknownHostException = "java.net.UnknownHostException: " + host;
                String illegalArgumentException = "java.lang.IllegalArgumentException: port out of range:" + port;
                String wrongPort = "java.net.ConnectException: Connection refused";
                if (unknownHostException.equals(cause)) {
                    String message = String.format("请确认ftp服务器地址是否正确，无法连接到地址为: [%s] 的ftp服务器", host);
                    LOG.error(message);
                    throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_LOGIN, message, e);
                } else if (illegalArgumentException.equals(cause) || wrongPort.equals(cause)) {
                    String message = String.format("请确认连接ftp服务器端口是否正确，错误的端口: [%s] ", port);
                    LOG.error(message);
                    throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_LOGIN, message, e);
                }
            } else {
                if ("Auth fail".equals(e.getMessage())) {
                    String message = String.format("与ftp服务器建立连接失败,请检查用户名和密码是否正确: [%s]",
                            "message:host =" + host + ",username = " + username + ",port =" + port);
                    LOG.error(message);
                    throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_LOGIN, message);
                } else {
                    String message = String.format("与ftp服务器建立连接失败 : [%s]",
                            "message:host =" + host + ",username = " + username + ",port =" + port);
                    LOG.error(message);
                    throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_LOGIN, message, e);
                }
            }
        }

    }

    @Override
    public void logoutFtpServer() {
        if (channelSftp != null) {
            channelSftp.disconnect();
        }
        if (session != null) {
            session.disconnect();
        }
    }

    @Override
    public boolean isDirExist(String directoryPath) {
        try {
            SftpATTRS sftpATTRS = channelSftp.lstat(directoryPath);
            return sftpATTRS.isDir();
        } catch (SftpException e) {
            if (e.getMessage().toLowerCase().equals("no such file")) {
                String message = String.format("请确认您的配置项path:[%s]存在，且配置的用户有权限读取", directoryPath);
                LOG.error(message);
                throw DataXException.asDataXException(FtpReaderErrorCode.FILE_NOT_EXISTS, message);
            }
            String message = String.format("进入目录：[%s]时发生I/O异常,请确认与ftp服务器的连接正常", directoryPath);
            LOG.error(message);
            throw DataXException.asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
        }
    }

    @Override
    public boolean isFileExist(String filePath) {
        boolean isExitFlag = false;
        try {
            SftpATTRS sftpATTRS = channelSftp.lstat(filePath);
            if (sftpATTRS.getSize() >= 0) {
                isExitFlag = true;
            }
        } catch (SftpException e) {
            if (e.getMessage().toLowerCase().equals("no such file")) {
                String message = String.format("请确认您的配置项path:[%s]存在，且配置的用户有权限读取", filePath);
                LOG.error(message);
                throw DataXException.asDataXException(FtpReaderErrorCode.FILE_NOT_EXISTS, message);
            } else {
                String message = String.format("获取文件：[%s] 属性时发生I/O异常,请确认与ftp服务器的连接正常", filePath);
                LOG.error(message);
                throw DataXException.asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
            }
        }
        return isExitFlag;
    }

    @Override
    public boolean isSymbolicLink(String filePath) {
        try {
            SftpATTRS sftpATTRS = channelSftp.lstat(filePath);
            return sftpATTRS.isLink();
        } catch (SftpException e) {
            if (e.getMessage().toLowerCase().equals("no such file")) {
                String message = String.format("请确认您的配置项path:[%s]存在，且配置的用户有权限读取", filePath);
                LOG.error(message);
                throw DataXException.asDataXException(FtpReaderErrorCode.FILE_NOT_EXISTS, message);
            } else {
                String message = String.format("获取文件：[%s] 属性时发生I/O异常,请确认与ftp服务器的连接正常", filePath);
                LOG.error(message);
                throw DataXException.asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
            }
        }
    }

    HashSet<Res> sourceFiles = new HashSet<Res>();

    @Override
    public HashSet<Res> getListFiles(String directoryPath, int parentLevel, int maxTraversalLevel) {
        if (parentLevel < maxTraversalLevel) {
            String parentPath = null;// 父级目录,以'/'结尾
            int pathLen = directoryPath.length();
            if (directoryPath.contains("*") || directoryPath.contains("?")) {//*和？的限制
                // path是正则表达式
                String subPath = UnstructuredStorageReaderUtil.getRegexPathParentPath(directoryPath);
                if (isDirExist(subPath)) {
                    parentPath = subPath;
                } else {
                    String message = String.format("不能进入目录：[%s]," + "请确认您的配置项path:[%s]存在，且配置的用户有权限进入", subPath,
                            directoryPath);
                    LOG.error(message);
                    throw DataXException.asDataXException(FtpReaderErrorCode.FILE_NOT_EXISTS, message);
                }

            } else if (isDirExist(directoryPath)) {
                // path是目录
                if (directoryPath.charAt(pathLen - 1) == IOUtils.DIR_SEPARATOR) {
                    parentPath = directoryPath;
                } else {
                    parentPath = directoryPath + IOUtils.DIR_SEPARATOR;
                }
            } else if (isSymbolicLink(directoryPath)) {
                //path是链接文件
                String message = String.format("文件:[%s]是链接文件，当前不支持链接文件的读取", directoryPath);
                LOG.error(message);
                throw DataXException.asDataXException(FtpReaderErrorCode.LINK_FILE, message);
            } else if (isFileExist(directoryPath)) {
                // path指向具体文件
                sourceFiles.add(new Res(directoryPath, directoryPath));
                return sourceFiles;
            } else {
                String message = String.format("请确认您的配置项path:[%s]存在，且配置的用户有权限读取", directoryPath);
                LOG.error(message);
                throw DataXException.asDataXException(FtpReaderErrorCode.FILE_NOT_EXISTS, message);
            }

            try {
                Vector vector = channelSftp.ls(directoryPath);
                for (int i = 0; i < vector.size(); i++) {
                    LsEntry le = (LsEntry) vector.get(i);
                    String strName = le.getFilename();
                    String filePath = parentPath + strName;

                    if (isDirExist(filePath)) {
                        // 是子目录
                        if (!(strName.equals(".") || strName.equals(".."))) {
                            //递归处理
                            getListFiles(filePath, parentLevel + 1, maxTraversalLevel);
                        }
                    } else if (isSymbolicLink(filePath)) {
                        //是链接文件
                        String message = String.format("文件:[%s]是链接文件，当前不支持链接文件的读取", filePath);
                        LOG.error(message);
                        throw DataXException.asDataXException(FtpReaderErrorCode.LINK_FILE, message);
                    } else if (isFileExist(filePath)) {
                        // 是文件
                        sourceFiles.add(new Res(filePath, filePath));
                    } else {
                        String message = String.format("请确认path:[%s]存在，且配置的用户有权限读取", filePath);
                        LOG.error(message);
                        throw DataXException.asDataXException(FtpReaderErrorCode.FILE_NOT_EXISTS, message);
                    }

                } // end for vector
            } catch (SftpException e) {
                String message = String.format("获取path：[%s] 下文件列表时发生I/O异常,请确认与ftp服务器的连接正常", directoryPath);
                LOG.error(message);
                throw DataXException.asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
            }

            return sourceFiles;
        } else {
            //超出最大递归层数
            String message = String.format("获取path：[%s] 下文件列表时超出最大层数,请确认路径[%s]下不存在软连接文件", directoryPath, directoryPath);
            LOG.error(message);
            throw DataXException.asDataXException(FtpReaderErrorCode.OUT_MAX_DIRECTORY_LEVEL, message);
        }
    }

    @Override
    public InputStream getInputStream(String filePath) {
        try {
            return channelSftp.get(filePath);
        } catch (SftpException e) {
            String message = String.format("读取文件 : [%s] 时出错,请确认文件：[%s]存在且配置的用户有权限读取", filePath, filePath);
            LOG.error(message);
            throw DataXException.asDataXException(FtpReaderErrorCode.OPEN_FILE_ERROR, message);
        }
    }

    @Override
    public void mkDirRecursive(String directoryPath) {
        boolean isDirExist = false;
        try {
            this.printWorkingDirectory();
            SftpATTRS sftpATTRS = this.channelSftp.lstat(directoryPath);
            isDirExist = sftpATTRS.isDir();
        } catch (SftpException e) {
            if (e.getMessage().toLowerCase().equals("no such file")) {
                LOG.warn(String.format(
                        "您的配置项path:[%s]不存在，将尝试进行目录创建, errorMessage:%s",
                        directoryPath, e.getMessage()), e);
                isDirExist = false;
            }
        }
        if (!isDirExist) {
            StringBuilder dirPath = new StringBuilder();
            dirPath.append(IOUtils.DIR_SEPARATOR_UNIX);
            String[] dirSplit = StringUtils.split(directoryPath, IOUtils.DIR_SEPARATOR_UNIX);
            try {
                // ftp server不支持递归创建目录,只能一级一级创建
                for (String dirName : dirSplit) {
                    dirPath.append(dirName);
                    mkDirSingleHierarchy(dirPath.toString());
                    dirPath.append(IOUtils.DIR_SEPARATOR_UNIX);
                }
            } catch (SftpException e) {
                String message = String
                        .format("创建目录:%s时发生I/O异常,请确认与ftp服务器的连接正常,拥有目录创建权限, errorMessage:%s",
                                directoryPath, e.getMessage());
                LOG.error(message, e);
                throw DataXException
                        .asDataXException(
                                FtpWriterErrorCode.COMMAND_FTP_IO_EXCEPTION,
                                message, e);
            }
        }
    }

    private boolean mkDirSingleHierarchy(String directoryPath) throws SftpException {
        boolean isDirExist = false;
        try {
            SftpATTRS sftpATTRS = this.channelSftp.lstat(directoryPath);
            isDirExist = sftpATTRS.isDir();
        } catch (SftpException e) {
            if (!isDirExist) {
                LOG.info(String.format("正在逐级创建目录 [%s]", directoryPath));
                this.channelSftp.mkdir(directoryPath);
                return true;
            }
        }
        if (!isDirExist) {
            LOG.info(String.format("正在逐级创建目录 [%s]", directoryPath));
            this.channelSftp.mkdir(directoryPath);
        }
        return true;
    }

    @Override
    public Set<String> getAllFilesInDir(String dir, String prefixFileName) {
        Set<String> allFilesWithPointedPrefix = new HashSet<String>();
        try {
            this.printWorkingDirectory();
            @SuppressWarnings("rawtypes")
            Vector allFiles = this.channelSftp.ls(dir);
            LOG.debug(String.format("ls: %s", JSON.toJSONString(allFiles,
                    SerializerFeature.UseSingleQuotes)));
            for (int i = 0; i < allFiles.size(); i++) {
                LsEntry le = (LsEntry) allFiles.get(i);
                String strName = le.getFilename();
                if (strName.startsWith(prefixFileName)) {
                    allFilesWithPointedPrefix.add(strName);
                }
            }
        } catch (SftpException e) {
            String message = String
                    .format("获取path:[%s] 下文件列表时发生I/O异常,请确认与ftp服务器的连接正常,拥有目录ls权限, errorMessage:%s",
                            dir, e.getMessage());
            LOG.error(message);
            throw DataXException.asDataXException(
                    FtpWriterErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
        }
        return allFilesWithPointedPrefix;
    }

    @Override
    public void deleteFiles(Set<String> filesToDelete) {
        String eachFile = null;
        try {
            this.printWorkingDirectory();
            for (String each : filesToDelete) {
                LOG.info(String.format("delete file [%s].", each));
                eachFile = each;
                this.channelSftp.rm(each);
            }
        } catch (SftpException e) {
            String message = String.format(
                    "删除文件:[%s] 时发生异常,请确认指定文件有删除权限,以及网络交互正常, errorMessage:%s",
                    eachFile, e.getMessage());
            LOG.error(message);
            throw DataXException.asDataXException(
                    FtpWriterErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
        }
    }

    private void printWorkingDirectory() {
        try {
            LOG.info(String.format("current working directory:%s",
                    this.channelSftp.pwd()));
        } catch (Exception e) {
            LOG.warn(String.format("printWorkingDirectory error:%s",
                    e.getMessage()));
        }
    }

    @Override
    public OutputStream getOutputStream(String filePath, boolean append) {
        try {
            this.printWorkingDirectory();
            String parentDir = filePath.substring(0,
                    StringUtils.lastIndexOf(filePath, IOUtils.DIR_SEPARATOR));
            this.channelSftp.cd(parentDir);
            this.printWorkingDirectory();
            OutputStream writeOutputStream = this.channelSftp.put(filePath,
                    append ? ChannelSftp.APPEND : ChannelSftp.OVERWRITE);
            String message = String.format(
                    "打开FTP文件[%s]获取写出流时出错,请确认文件%s有权限创建，有权限写出等", filePath,
                    filePath);
            if (null == writeOutputStream) {
                throw DataXException.asDataXException(
                        FtpWriterErrorCode.OPEN_FILE_ERROR, message);
            }
            return writeOutputStream;
        } catch (SftpException e) {
            String message = String.format(
                    "写出文件[%s] 时出错,请确认文件%s有权限写出, errorMessage:%s", filePath,
                    filePath, e.getMessage());
            LOG.error(message);
            throw DataXException.asDataXException(
                    FtpWriterErrorCode.OPEN_FILE_ERROR, message);
        }
    }
}
