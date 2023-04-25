package com.alibaba.datax.common.scala.element

import java.sql.{Time, Timestamp}
import java.util.{Date, TimeZone}

import com.alibaba.datax.common.exception.{CommonErrorCode, DataXException}
import com.alibaba.datax.common.util.Configuration
import org.apache.commons.lang3.time.DateFormatUtils

/**
 * 后续为了可维护性，可以考虑直接使用 apache 的DateFormatUtils.
 * <p>
 * 迟南已经修复了该问题，但是为了维护性，还是直接使用apache的内置函数
 */
object DateCast {
  private[element] var datetimeFormat = "yyyy-MM-dd HH:mm:ss"
  private[element] var dateFormat = "yyyy-MM-dd"
  private[element] var timeFormat = "HH:mm:ss"
  private[element] var timeZone = "GMT+8"
  private[element] var timeZoner = TimeZone.getTimeZone(DateCast.timeZone)

  def init(configuration: Configuration) = {
    DateCast.datetimeFormat = configuration.getString("common.column.datetimeFormat", datetimeFormat)
    DateCast.timeFormat = configuration.getString("common.column.timeFormat", timeFormat)
    DateCast.dateFormat = configuration.getString("common.column.dateFormat", dateFormat)
    DateCast.timeZone = configuration.getString("common.column.timeZone", DateCast.timeZone)
    DateCast.timeZoner = TimeZone.getTimeZone(DateCast.timeZone)
  }

  def asString(column: Column): String = {
    if (null == column.asDate) return null
    column match {
      case TimeColumn(stamp: Time) =>
        DateFormatUtils.format(stamp, DateCast.dateFormat, DateCast.timeZoner)
      case DateColumn(date: Date) =>
        DateFormatUtils.format(date, DateCast.timeFormat, DateCast.timeZoner)
      case TimeStampColumn(stamp: Timestamp) =>
        DateFormatUtils.format(stamp, DateCast.datetimeFormat, DateCast.timeZoner)
      case _ =>
        throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "时间类型出现不支持类型，目前仅支持DATE/TIME/DATETIME。该类型属于编程错误，请反馈给DataX开发团队 .")
    }
  }
}
