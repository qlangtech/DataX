package com.alibaba.datax.common.scala.element

import java.lang
import java.math.BigInteger
import java.sql.Timestamp
import java.util.Date

/**
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-04-25 16:41
 **/
case class TimeStampColumn(val stamp: Timestamp) extends AnyVal with Column {
  override def isNull: Boolean = stamp == null

  override def asLong: lang.Long = ???

  override def asDouble: lang.Double = ???

  override def asString: String = ???

  override def asDate: Date = this.stamp

  override def asDate(dateFormat: String): Date = this.stamp

  override def asBytes: Array[Byte] = ???

  override def asBoolean: lang.Boolean = ???

  override def asBigDecimal: java.math.BigDecimal = ???

  override def asBigInteger: BigInteger = ???

  override def getByteSize: Integer = ???
}
