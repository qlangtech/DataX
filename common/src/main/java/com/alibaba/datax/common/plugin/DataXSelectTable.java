package com.alibaba.datax.common.plugin;

import com.qlangtech.tis.plugin.ds.ISelectedTab;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-07-28 09:58
 **/
public class DataXSelectTable {
    private final ISelectedTab selectedTab;

    public DataXSelectTable(ISelectedTab selectedTab) {
        this.selectedTab = selectedTab;
    }

    public Optional<String> getWhere() {
        return Optional.ofNullable(this.selectedTab.getWhere());
    }
}
