package com.alibaba.datax.plugin.writer.elasticsearchwriter;

import com.qlangtech.tis.datax.IDataxProcessor;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-07-06 21:02
 **/
public interface IInitialElasticSearchIndex {
    public List<ESColumn> initialIndex(IDataxProcessor dataxProcessor);
}
