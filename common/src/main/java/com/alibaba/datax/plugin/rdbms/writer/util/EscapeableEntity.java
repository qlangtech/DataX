package com.alibaba.datax.plugin.rdbms.writer.util;

import org.apache.commons.lang3.StringUtils;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-10-03 10:38
 **/
public class EscapeableEntity {
    protected final String escapeChar;
    protected final boolean containEscapeChar;

    public EscapeableEntity(String escapeChar) {
        this.escapeChar = escapeChar;
        this.containEscapeChar = StringUtils.isNotEmpty(escapeChar);
    }

    public String getEscapeChar() {
        return escapeChar;
    }

    public boolean isContainEscapeChar() {
        return containEscapeChar;
    }

    protected String escapeEntity(String val) {
        if (containEscapeChar) {
            return escapeChar + val + escapeChar;
        } else {
            return val;
        }
    }
}
