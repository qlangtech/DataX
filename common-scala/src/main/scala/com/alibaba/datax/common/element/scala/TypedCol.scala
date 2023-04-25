package com.alibaba.datax.common.element.scala
import java.math.{BigDecimal, BigInteger}
import java.util.Date

/**
 *
 */
trait TypedCol extends Any{
  def asLong: Long

  def asDouble: Double

  def asString: String

  def asDate: Date

  def asDate(dateFormat: String): Date

  def asBytes: Array[Byte]

  def asBoolean: Boolean

  def asBigDecimal: BigDecimal

  def asBigInteger: BigInteger
}
