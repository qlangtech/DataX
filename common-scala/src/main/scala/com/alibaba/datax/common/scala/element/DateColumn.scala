package com.alibaba.datax.common.scala.element

import java.lang
import java.math.BigInteger
import java.util.Date

import com.alibaba.datax.common.element.Column

/**
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-04-25 16:26
 **/
case class DateColumn(val date: Date) extends AnyVal with Column {
  override def isNull: Boolean = this.date == null


  override def getType(): Column.Type = Column.Type.DATE

  override def asLong: lang.Long = date.getTime

  override def asDouble: lang.Double = ???

  override def asString: String = DateCast.asString(this)

  override def asDate: Date = this.date

  override def asDate(dateFormat: String): Date = this.date

  override def asBytes: Array[Byte] = ???

  override def asBoolean: lang.Boolean = ???

  override def asBigDecimal: java.math.BigDecimal = ???

  override def asBigInteger: BigInteger = BigInteger.valueOf(date.getTime)

  override def getByteSize: Integer = 0
}
