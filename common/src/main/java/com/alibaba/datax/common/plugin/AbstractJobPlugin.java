package com.alibaba.datax.common.plugin;

import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.IDataxWriter;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

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
        DataXName dataXName = this.containerContext.getTISDataXName();
        IDataxProcessor processor = DataxProcessor.load(null, dataXName.getType(), dataXName.getPipelineName());
        return processor;
    }

    protected final IDataxWriter loadDataXWriter() {
        return this.loadProcessor().getWriter(null);
    }

    protected final IDataxReader loadDataXReader() {
        return this.loadProcessor().getReader(null);
    }

    public final DataXSelectTable loadSelectTable(String tableName) {
        List<ISelectedTab> tabs = loadDataXReader().getUnfilledSelectedTabs();
        for (ISelectedTab tab : tabs) {
            if (StringUtils.equals(tab.getName(), tableName)) {
                return new DataXSelectTable(tab);
            }
        }
        throw new IllegalStateException("can not find tab:" + tableName + ",in tabs:"
                + tabs.stream().map((tab) -> tab.getSourceTableName()).collect(Collectors.joining(",")));
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
