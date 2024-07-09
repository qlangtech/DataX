package com.alibaba.datax.common.plugin;

import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.IDataxWriter;
import com.qlangtech.tis.datax.impl.DataxProcessor;

/**
 * Created by jingxing on 14-8-24.
 */
public abstract class AbstractJobPlugin extends AbstractPlugin {


    /**
     * @return the jobPluginCollector
     */
    public JobPluginCollector getJobPluginCollector() {
        return jobPluginCollector;
    }

    protected final IDataxProcessor loadProcessor() {
        IDataxProcessor processor = DataxProcessor.load(null, this.containerContext.getTISDataXName());
        return processor;
    }

    protected final IDataxWriter loadDataXWriter() {
        return this.loadProcessor().getWriter(null);
    }

    /**
     * @param jobPluginCollector the jobPluginCollector to set
     */
    public void setJobPluginCollector(
            JobPluginCollector jobPluginCollector) {
        this.jobPluginCollector = jobPluginCollector;
    }

    private JobPluginCollector jobPluginCollector;

}
