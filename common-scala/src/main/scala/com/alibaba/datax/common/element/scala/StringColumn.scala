package com.alibaba.datax.common.element.scala
import java.math.{BigDecimal, BigInteger}
import java.util.Date


case class StringColumn(colVal: String) extends AnyVal with TypedCol {
  override def asLong: Long = 1;

  override def asDouble: Double = 1

  override def asString: String = ""

  override def asDate: Date = null

  override def asDate(dateFormat: String): Date = null

  override def asBytes: Array[Byte] = null

  override def asBoolean: Boolean = false

  override def asBigDecimal: BigDecimal = null

  override def asBigInteger: BigInteger = null
}
