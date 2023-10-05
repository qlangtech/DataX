package com.alibaba.datax.plugin.writer.elasticsearchwriter;

import com.alibaba.datax.common.element.Column;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/10/5
 */
public class DataConvertUtils {
    public static String getDateStr(ESColumn esColumn, Column column) {
        DateTime date = null;
        DateTimeZone dtz = DateTimeZone.getDefault();
        if (esColumn.getTimezone() != null) {
            // 所有时区参考 http://www.joda.org/joda-time/timezones.html
            dtz = DateTimeZone.forID(esColumn.getTimezone());
        }
        if (column.getType() != Column.Type.DATE && esColumn.getFormat() != null) {
            DateTimeFormatter formatter = DateTimeFormat.forPattern(esColumn.getFormat());
            date = formatter.withZone(dtz).parseDateTime(column.asString());
            return date.toString();
        } else if (column.getType() == Column.Type.DATE) {
            date = new DateTime(column.asLong(), dtz);
            return date.toString();
        } else {
            return column.asString();
        }
    }
}
