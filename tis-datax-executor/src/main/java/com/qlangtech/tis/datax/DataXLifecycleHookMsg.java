package com.qlangtech.tis.datax;

import com.qlangtech.tis.plugin.StoreResourceType;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/18
 */
public class DataXLifecycleHookMsg implements IDataXTaskRelevant {

    private IDataXBatchPost.LifeCycleHook lifeCycleHook;
    private String tableName;
    private StoreResourceType resType;
    private String dataXName;
    private String jobName;
    private Integer taskId;
    private Long execEpochMilli;

    private DataXLifecycleHookMsg() {
    }

    public static DataXLifecycleHookMsg createDataXLifecycleHookMsg(String dataXName, String tableName,
                                                                    Integer taskId, String jobName,
                                                                    Long currentTimeStamp,
                                                                    IDataXBatchPost.LifeCycleHook lifeCycleHook) {
        if (StringUtils.isEmpty(dataXName)) {
            throw new IllegalArgumentException("dataXName can not be null");
        }
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("tableName can not be null");
        }
        if (StringUtils.isEmpty(jobName)) {
            throw new IllegalArgumentException("jobName can not be null");
        }

        DataXLifecycleHookMsg lifecycleHookMsg = new DataXLifecycleHookMsg();
        lifecycleHookMsg.setTableName(tableName);
        lifecycleHookMsg.setLifeCycleHook(lifeCycleHook);
        lifecycleHookMsg.setResType(StoreResourceType.DataApp);
        lifecycleHookMsg.setTaskId(Objects.requireNonNull(taskId, "taskId can not be null"));
        lifecycleHookMsg.setDataXName(dataXName);
        lifecycleHookMsg.setExecEpochMilli(Objects.requireNonNull(currentTimeStamp, "currentTimeStamp can not be " +
                "null"));
        lifecycleHookMsg.setJobName(jobName);
        return lifecycleHookMsg;
    }

    private void setDataXName(String dataXName) {
        this.dataXName = dataXName;
    }

    private void setJobName(String jobName) {
        this.jobName = jobName;
    }

    private void setTaskId(Integer taskId) {
        this.taskId = taskId;
    }

    private void setExecEpochMilli(Long execEpochMilli) {
        this.execEpochMilli = execEpochMilli;
    }

    public String getTableName() {
        return this.tableName;
    }

    public StoreResourceType getResType() {
        return resType;
    }

    private void setResType(StoreResourceType resType) {
        this.resType = resType;
    }

    private void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public IDataXBatchPost.LifeCycleHook getLifeCycleHook() {
        return Objects.requireNonNull(this.lifeCycleHook, "lifeCycleHook can not be null");
    }

    private void setLifeCycleHook(IDataXBatchPost.LifeCycleHook lifeCycleHook) {
        this.lifeCycleHook = lifeCycleHook;
    }

    @Override
    public Integer getTaskId() {
        return Objects.requireNonNull(this.taskId, "taskid can not be null");
    }

    @Override
    public String getJobName() {
        return this.jobName;
    }

    @Override
    public String getDataXName() {
        return this.dataXName;
    }

    @Override
    public long getExecEpochMilli() {
        //  throw new UnsupportedOperationException();
        return Objects.requireNonNull(this.execEpochMilli, "execEpochMilli can not be null");
    }

    @Override
    public int getTaskSerializeNum() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getFormatTime(TimeFormat format) {
        throw new UnsupportedOperationException();
    }
}
