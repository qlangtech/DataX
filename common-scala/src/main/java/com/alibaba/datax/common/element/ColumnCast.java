package com.alibaba.datax.common.element;

import com.alibaba.datax.common.scala.element.BytesColumn;
import com.alibaba.datax.common.scala.element.DateCast;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.time.FastDateFormat;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

public final class ColumnCast {

    public static void bind(final Configuration configuration) {
        StringCast.init(configuration);
        DateCast.init(configuration);
        BytesCast.init(configuration);
    }

//    public static Date string2Date(final StringColumn column)
//            throws ParseException {
//        return StringCast.asDate(column);
//    }
//
//    public static Date string2Date(final StringColumn column, String dateFormat)
//            throws ParseException {
//        return StringCast.asDate(column, dateFormat);
//    }
//
//    public static byte[] string2Bytes(final StringColumn column)
//            throws UnsupportedEncodingException {
//        return StringCast.asBytes(column);
//    }

//    public static String date2String(final com.alibaba.datax.common.scala.element.TimeColumn column) {
//        return DateCast.asString(column);
//    }

    public static String bytes2String(final com.alibaba.datax.common.scala.element.BytesColumn column)
            throws UnsupportedEncodingException {
        return BytesCast.asString(column);
    }
}

class StringCast {
    static String datetimeFormat = "yyyy-MM-dd HH:mm:ss";

    static String dateFormat = "yyyy-MM-dd";

    static String timeFormat = "HH:mm:ss";

    static List<String> extraFormats = Collections.emptyList();

    static String timeZone = "GMT+8";

    static FastDateFormat dateFormatter;

    static FastDateFormat timeFormatter;

    static FastDateFormat datetimeFormatter;

    static TimeZone timeZoner;

    static String encoding = "UTF-8";

    static void init(final Configuration configuration) {
        StringCast.datetimeFormat = configuration.getString(
                "common.column.datetimeFormat", StringCast.datetimeFormat);
        StringCast.dateFormat = configuration.getString(
                "common.column.dateFormat", StringCast.dateFormat);
        StringCast.timeFormat = configuration.getString(
                "common.column.timeFormat", StringCast.timeFormat);
        StringCast.extraFormats = configuration.getList(
                "common.column.extraFormats", Collections.<String>emptyList(), String.class);

        StringCast.timeZone = configuration.getString("common.column.timeZone",
                StringCast.timeZone);
        StringCast.timeZoner = TimeZone.getTimeZone(StringCast.timeZone);

        StringCast.datetimeFormatter = FastDateFormat.getInstance(
                StringCast.datetimeFormat, StringCast.timeZoner);
        StringCast.dateFormatter = FastDateFormat.getInstance(
                StringCast.dateFormat, StringCast.timeZoner);
        StringCast.timeFormatter = FastDateFormat.getInstance(
                StringCast.timeFormat, StringCast.timeZoner);

        StringCast.encoding = configuration.getString("common.column.encoding",
                StringCast.encoding);
    }

//    static Date asDate(final StringColumn column) throws ParseException {
//        if (null == column.asString()) {
//            return null;
//        }
//
//
//        try {
//            return StringCast.datetimeFormatter.parse(column.asString());
//        } catch (ParseException e) {
//
//        }
//
//
//        try {
//            return StringCast.dateFormatter.parse(column.asString());
//        } catch (ParseException e) {
//
//        }
//
//
//        ParseException e;
//        try {
//            return StringCast.timeFormatter.parse(column.asString());
//        } catch (ParseException ignored) {
//            e = ignored;
//        }
//
//        for (String format : StringCast.extraFormats) {
//            return FastDateFormat.getInstance(format, StringCast.timeZoner).parse(column.asString());
//        }
//
//        throw new IllegalStateException();
//    }
//
//    static Date asDate(final StringColumn column, String dateFormat) throws ParseException {
//        ParseException e;
//        try {
//            return FastDateFormat.getInstance(dateFormat, StringCast.timeZoner).parse(column.asString());
//        } catch (ParseException ignored) {
//            e = ignored;
//        }
//        throw e;
//    }
//
//    static byte[] asBytes(final StringColumn column)
//            throws UnsupportedEncodingException {
//        if (null == column.asString()) {
//            return null;
//        }
//
//        return column.asString().getBytes(StringCast.encoding);
//    }
}

class BytesCast {
    static String encoding = "utf-8";

    static void init(final Configuration configuration) {
        BytesCast.encoding = configuration.getString("common.column.encoding",
                BytesCast.encoding);
        return;
    }

    static String asString(final BytesColumn column)
            throws UnsupportedEncodingException {
        if (null == column.asBytes()) {
            return null;
        }

        return new String(column.asBytes(), encoding);
    }
}
