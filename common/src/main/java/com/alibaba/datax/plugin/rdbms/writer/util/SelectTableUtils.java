package com.alibaba.datax.plugin.rdbms.writer.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.qlangtech.tis.plugin.ds.IDBReservedKeys;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-03-18 13:01
 **/
public class SelectTableUtils {
    public static SelectTable create(Configuration conf, IDBReservedKeys escapeChar) {
//        return new SelectTable(
//                conf.getString(String.format(
//                        "%s[0].%s[0]", Constant.CONN_MARK, Key.TABLE)), conf.get(Key.ESCAPE_CHAR, String.class));

        return SelectTable.create(conf.getString(String.format(
                "%s[0].%s[0]", Constant.CONN_MARK, Key.TABLE)), escapeChar);
    }

    public static SelectTable createInTask(Configuration conf, IDBReservedKeys escapeChar) {
        return new SelectTable(
                conf.getString(Key.TABLE), escapeChar);
    }
}
