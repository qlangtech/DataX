package com.alibaba.datax.core.util;

import com.alibaba.datax.core.job.ITransformerBuildInfo;
import com.alibaba.datax.core.transport.transformer.TransformerExecution;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-06-15 10:11
 **/
public interface TransformerBuildInfo extends ITransformerBuildInfo {

    List<TransformerExecution> getExecutions();


}
