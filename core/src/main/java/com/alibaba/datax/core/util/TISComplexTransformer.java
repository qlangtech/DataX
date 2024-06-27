package com.alibaba.datax.core.util;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.transformer.ComplexTransformer;
import com.qlangtech.tis.plugin.datax.transformer.UDFDefinition;

import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-06-13 18:39
 **/
public class TISComplexTransformer extends ComplexTransformer {
    private final  UDFDefinition tisUDF;

    public TISComplexTransformer(UDFDefinition tisUDF) {
        this.tisUDF = tisUDF;
    }

    @Override
    public Record evaluate(Record record, Map<String, Object> tContext, Object... paras) {
         tisUDF.evaluate(record);
         return record;
    }
}
