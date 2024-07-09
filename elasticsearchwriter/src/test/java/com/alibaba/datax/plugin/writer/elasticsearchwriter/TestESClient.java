package com.alibaba.datax.plugin.writer.elasticsearchwriter;

import com.alibaba.datax.plugin.writer.elasticsearchwriter.ESClient.SchemaCol;
import junit.framework.TestCase;
import org.junit.Assert;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-07-06 17:27
 **/
public class TestESClient extends TestCase {
    public void testGetMapping() throws Exception {
        String endpoint = "http://192.168.28.201:9200";
        ESInitialization config = (ESInitialization.create(endpoint, null, null,
                false,
                300000,
                false,
                false));
        ESClient esClient = new ESClient(config);
        List<SchemaCol> mapping = esClient.getMapping("video");
        Assert.assertTrue(mapping.size() > 0);
    }
}
