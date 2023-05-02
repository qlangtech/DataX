package com.alibaba.datax.common.scala.element

import java.io.UnsupportedEncodingException
import java.text.ParseException
import java.util
import java.util.{Collections, Date, TimeZone}

import com.alibaba.datax.common.util.Configuration
import org.apache.commons.lang3.time.FastDateFormat

/**
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-04-25 17:00
 **/
object StringCast {
  private[element] var datetimeFormat: String = "yyyy-MM-dd HH:mm:ss"
  private[element] var dateFormat: String = "yyyy-MM-dd"
  private[element] var timeFormat: String = "HH:mm:ss"
  private[element] var extraFormats: util.List[String] = new util.ArrayList[String]()
  private[element] var timeZone: String = "GMT+8"
  private[element] var dateFormatter: FastDateFormat = null
  private[element] var timeFormatter: FastDateFormat = null
  private[element] var datetimeFormatter: FastDateFormat = null
  private[element] var timeZoner: TimeZone = null
  private[element] var encoding: String = "UTF-8"

  private[element] def init(configuration: Configuration): Unit = {
    StringCast.datetimeFormat = configuration.getString("common.column.datetimeFormat", StringCast.datetimeFormat)
    StringCast.dateFormat = configuration.getString("common.column.dateFormat", StringCast.dateFormat)
    StringCast.timeFormat = configuration.getString("common.column.timeFormat", StringCast.timeFormat)
    StringCast.extraFormats = configuration.getList("common.column.extraFormats", Collections.emptyList[String], classOf[String])
    StringCast.timeZone = configuration.getString("common.column.timeZone", StringCast.timeZone)
    StringCast.timeZoner = TimeZone.getTimeZone(StringCast.timeZone)
    StringCast.datetimeFormatter = FastDateFormat.getInstance(StringCast.datetimeFormat, StringCast.timeZoner)
    StringCast.dateFormatter = FastDateFormat.getInstance(StringCast.dateFormat, StringCast.timeZoner)
    StringCast.timeFormatter = FastDateFormat.getInstance(StringCast.timeFormat, StringCast.timeZoner)
    StringCast.encoding = configuration.getString("common.column.encoding", StringCast.encoding)
  }

      def asDate(column: Column): Date = {
      if (null == column.asString) return null
      try return StringCast.datetimeFormatter.parse(column.asString)
      catch {
        case e: ParseException =>

      }
      try return StringCast.dateFormatter.parse(column.asString)
      catch {
        case e: ParseException =>

      }
      var e: ParseException = null
      try return StringCast.timeFormatter.parse(column.asString)
      catch {
        case ignored: ParseException =>
          e = ignored
      }
      import scala.collection.JavaConversions._
      for (format <- StringCast.extraFormats) {
        return FastDateFormat.getInstance(format, StringCast.timeZoner).parse(column.asString)
      }
      throw new IllegalStateException
    }

    @throws[ParseException]
    def asDate(column: Column, dateFormat: String): Date = {
      var e: ParseException = null
      try return FastDateFormat.getInstance(dateFormat, StringCast.timeZoner).parse(column.asString)
      catch {
        case ignored: ParseException =>
          e = ignored
      }
      throw e
    }

    @throws[UnsupportedEncodingException]
    private[element] def asBytes(column: Column): Array[Byte] = {
      if (null == column.asString) return null
      column.asString.getBytes(StringCast.encoding)
    }
}
