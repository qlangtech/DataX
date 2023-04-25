package com.alibaba.datax.core.util;

import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-04-25 14:56
 **/
public class RecordUtils {
    public static String toJSONString(Object val) {
        return com.alibaba.fastjson.JSON.toJSONString(val);
    }
}
