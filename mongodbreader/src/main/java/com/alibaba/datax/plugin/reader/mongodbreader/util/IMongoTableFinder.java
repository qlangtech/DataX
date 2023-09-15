package com.alibaba.datax.plugin.reader.mongodbreader.util;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/10
 */
public interface IMongoTableFinder {
    /**
     * @param tableName
     * @return
     */
    public IMongoTable findMongoTable(String tableName);


}
