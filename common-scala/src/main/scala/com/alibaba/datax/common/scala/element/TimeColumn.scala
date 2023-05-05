package com.alibaba.datax.common.scala.element

import java.sql.Time
import java.util.Date

import com.alibaba.datax.common.element.Column
import com.alibaba.datax.common.exception.{CommonErrorCode, DataXException}

/**
 * Created by jingxing on 14-8-24.
 */
//object DateColumn {
//
//  object DateType extends Enumeration {
//    type DateType = Value
//    val DATE, TIME, DATETIME = Value
//  }
//
//}

case class TimeColumn(val stamp: Time)

/**
 * 构建值为stamp(Unix时间戳)的DateColumn，使用Date子类型为DATETIME
 * 实际存储有date改为long的ms，节省存储
 **/
  extends AnyVal with Column {


  // private var subType = DateColumn.DateType.DATETIME

  //  /**
  //   * 构建值为null的DateColumn，使用Date子类型为DATETIME
  //   **/
  //  def this {
  //    this(null.asInstanceOf[Long])
  //  }
  //
  //  /**
  //   * 构建值为date(java.util.Date)的DateColumn，使用Date子类型为DATETIME
  //   **/
  //  def this(date: Date) {
  //    this(if (date == null) null
  //    else date.getTime)
  //  }
  //
  //  /**
  //   * 构建值为date(java.sql.Date)的DateColumn，使用Date子类型为DATE，只有日期，没有时间
  //   **/
  //  def this(date: Date) {
  //    this(if (date == null) null
  //    else date.getTime)
  //    this.setSubType(DateColumn.DateType.DATE)
  //  }
  //
  //  /**
  //   * 构建值为time(java.sql.Time)的DateColumn，使用Date子类型为TIME，只有时间，没有日期
  //   **/
  //  def this(time: Time) {
  //    this(if (time == null) null
  //    else time.getTime)
  //    this.setSubType(DateColumn.DateType.TIME)
  //  }
  //
  //  /**
  //   * 构建值为ts(java.sql.Timestamp)的DateColumn，使用Date子类型为DATETIME
  //   **/
  //  def this(ts: Timestamp) {
  //    this(if (ts == null) null
  //    else ts.getTime)
  //    this.setSubType(DateColumn.DateType.DATETIME)
  //  }

  override def getType(): Column.Type = Column.Type.DATE

  override def asLong = this.stamp.getTime //this.getRawData.asInstanceOf[Long]

  override def asString = try DateCast.asString(this)
  catch {
    case e: Exception =>
      throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, String.format("Date[%s]类型不能转为String .", this.toString))
  }

  override def asDate: Date = {
    if (null == this.stamp) return null
    new Date(this.stamp.getTime)
  }

  override def asDate(dateFormat: String) = asDate

  override def asBytes = throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Date类型不能转为Bytes .")

  override def asBoolean = throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Date类型不能转为Boolean .")

  override def asDouble = throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Date类型不能转为Double .")

  override def asBigInteger = throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Date类型不能转为BigInteger .")

  override def asBigDecimal = throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Date类型不能转为BigDecimal .")

  //  def getSubType = subType
  //
  //  def setSubType(subType: DateColumn.DateType) = this.subType = subType
  override def isNull: Boolean = this.stamp == null

  override def getByteSize: Integer = 0
}
