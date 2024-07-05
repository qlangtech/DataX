package com.alibaba.datax.common.plugin;

import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.impl.DataxProcessor;

/**
 * Created by jingxing on 14-8-24.
 */
public abstract class AbstractTaskPlugin extends AbstractPlugin {

    //TaskPlugin 应该具备taskId
    private int taskGroupId;
    private int taskId;
    private TaskPluginCollector taskPluginCollector;

    public TaskPluginCollector getTaskPluginCollector() {
        return taskPluginCollector;
    }

    public void setTaskPluginCollector(
            TaskPluginCollector taskPluginCollector) {
        this.taskPluginCollector = taskPluginCollector;
    }

    protected final IDataxReader loadDataXReader() {
        IDataxProcessor dataxProcessor = DataxProcessor.load(null, this.containerContext.getTISDataXName());
        IDataxReader dataxReader = dataxProcessor.getReader(null);
        return dataxReader;
    }


    public int getTaskId() {
        return taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public int getTaskGroupId() {
        return taskGroupId;
    }

    public void setTaskGroupId(int taskGroupId) {
        this.taskGroupId = taskGroupId;
    }
}
