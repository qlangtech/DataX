package com.alibaba.datax.plugin.rdbms.writer.util;

import com.alibaba.datax.common.element.DirtyRecordCreator;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter.Task;
import com.alibaba.datax.plugin.rdbms.writer.Convert2InsertSQL;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.util.function.Consumer;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-10-13 09:17
 **/
public class DefaultDirtyRecordCreator implements DirtyRecordCreator {
    private final CommonRdbmsWriter.Task task;
    private final Record record;
    private static final Logger logger = LoggerFactory.getLogger(DefaultDirtyRecordCreator.class);
    private final Consumer<PreparedStatement> fillPreparedStatement;

    public DefaultDirtyRecordCreator(Task task, Record record, Consumer<PreparedStatement> fillPreparedStatement) {
        this.task = task;
        this.record = record;
        this.fillPreparedStatement = fillPreparedStatement;
    }

    @Override
    public Object createDirtyRecordDescriptor() {
        // convert to insert SQL
        try {
            Convert2InsertSQL convert2InsertSQL = new Convert2InsertSQL(task.getWriteRecordSql(), task.getColumnNumber(), record);
            fillPreparedStatement.accept(convert2InsertSQL);
            return convert2InsertSQL.getFinalSQL();
        } catch (Exception e) {
            logger.warn(e.getMessage(), ExceptionUtils.getRootCause(e));
            return StringUtils.EMPTY;
        }

    }

    @Override
    public int getByteSize() {
        return record.getByteSize();
    }

    @Override
    public int getMemorySize() {
        return record.getMemorySize();
    }
}
