package com.alibaba.datax.plugin.reader.mongodbreader.util;

import com.alibaba.datax.common.element.Record;
import org.bson.Document;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/10
 */
public interface IMongoTable {
    public Document getCollectionQueryFilter();

    /**
     * mongo 展示列
     *
     * @return
     */
    //public List<IMongoCol> getMongoPresentCols();

    Record convert2RecordByItem(Record record, Document item);

}
