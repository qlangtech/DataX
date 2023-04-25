package com.alibaba.datax.common.scala.element

import java.lang
import java.math.BigInteger
import java.util.Date

/**
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-04-25 16:26
 **/
case class DateColumn(val date: Date) extends AnyVal with Column {
  override def isNull: Boolean = ???

  override def asLong: lang.Long = ???

  override def asDouble: lang.Double = ???

  override def asString: String = DateCast.asString(this)

  override def asDate: Date = this.date

  override def asDate(dateFormat: String): Date = ???

  override def asBytes: Array[Byte] = ???

  override def asBoolean: lang.Boolean = ???

  override def asBigDecimal: java.math.BigDecimal = ???

  override def asBigInteger: BigInteger = ???

  override def getByteSize: Integer = ???
}
