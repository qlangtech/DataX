package com.alibaba.datax.plugin.rdbms.writer.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import org.apache.commons.lang3.StringUtils;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-10-03 10:34
 **/
public class SelectTable extends EscapeableEntity {

    private final String tabName;

    public static SelectTable create(Configuration conf) {
//        return new SelectTable(
//                conf.getString(String.format(
//                        "%s[0].%s[0]", Constant.CONN_MARK, Key.TABLE)), conf.get(Key.ESCAPE_CHAR, String.class));

        return create(conf.getString(String.format(
                "%s[0].%s[0]", Constant.CONN_MARK, Key.TABLE)), conf);
    }

    public static SelectTable create(String table, Configuration conf) {
        return new SelectTable(
                table, conf.getString(Key.ESCAPE_CHAR));
    }

    public static SelectTable createInTask(Configuration conf) {
        return new SelectTable(
                conf.getString(Key.TABLE), conf.getString(Key.ESCAPE_CHAR));
    }


    public String getTabName() {
        return this.escapeEntity(tabName);
    }

    public String getUnescapeTabName() {
        return this.tabName;
    }

    @Override
    public String toString() {
        return getTabName();
    }

    private SelectTable(String tabName, String escapeChar) {
        super(escapeChar);
        if (StringUtils.isEmpty(tabName)) {
            throw new IllegalArgumentException("param tabName can not be empty");
        }
        this.tabName = tabName;
    }
}
