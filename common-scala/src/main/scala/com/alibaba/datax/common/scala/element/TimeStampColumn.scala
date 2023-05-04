package com.alibaba.datax.common.scala.element

import java.lang
import java.math.BigInteger
import java.sql.Timestamp
import java.util.Date

import com.alibaba.datax.common.exception.{CommonErrorCode, DataXException}

/**
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-04-25 16:41
 **/
case class TimeStampColumn(val stamp: Timestamp) extends AnyVal with Column {
  override def isNull: Boolean = stamp == null

  override def asLong: lang.Long = throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Date类型不能转为BigDecimal .")

  override def asDouble: lang.Double = throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Date类型不能转为BigDecimal .")

  override def asString: String = try {
    DateCast.asString(this)
  }
  catch {
    case e: Exception =>
      throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, String.format("Date[%s]类型不能转为String .", this.toString))
  }

  override def asDate: Date = this.stamp

  override def asDate(dateFormat: String): Date = this.stamp

  override def asBytes: Array[Byte] = throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Date类型不能转为BigDecimal .")

  override def asBoolean: lang.Boolean = throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Date类型不能转为BigDecimal .")

  override def asBigDecimal: java.math.BigDecimal = throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Date类型不能转为BigDecimal .")

  override def asBigInteger: BigInteger = throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Date类型不能转为BigDecimal .")

  override def getByteSize: Integer = 0
}
