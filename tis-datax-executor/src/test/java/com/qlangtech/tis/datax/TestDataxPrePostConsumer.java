package com.qlangtech.tis.datax;

import junit.framework.TestCase;

import java.util.Objects;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/20
 */
public class TestDataxPrePostConsumer extends TestCase {

    String tableName = "totalpayinfo";
    Integer taskId = 1;
    String dataXName = "mysql_hive3";
    Long currentTimeStamp = TimeFormat.getCurrentTimeStamp();

    public void testConsumePreExecMessage() throws Exception {


        DataxPrePostConsumer prePostConsumer = new DataxPrePostConsumer();

        DataXLifecycleHookMsg lifecycleHookMsg = createHookMsg(IDataXBatchPost.LifeCycleHook.Prep);

        prePostConsumer.consumeMessage(lifecycleHookMsg);
    }

    private DataXLifecycleHookMsg createHookMsg(IDataXBatchPost.LifeCycleHook lifeCycleHook) {
        Objects.requireNonNull(lifeCycleHook, "lifeCycleHook can not be null");
        return DataXLifecycleHookMsg.createDataXLifecycleHookMsg(this.dataXName, tableName, taskId,IDataXBatchPost.KEY_PREP + tableName ,currentTimeStamp
                , lifeCycleHook);
    }

    public void testConsumePostExecMessage() throws Exception {
        DataxPrePostConsumer prePostConsumer = new DataxPrePostConsumer();

        DataXLifecycleHookMsg lifecycleHookMsg = createHookMsg(IDataXBatchPost.LifeCycleHook.Post);

        prePostConsumer.consumeMessage(lifecycleHookMsg);
    }

}
