package com.alibaba.datax.common.scala.element

import java.math.{BigDecimal, BigInteger}
import java.util.Date

/**
 *
 */
trait Column extends Any {
  def isNull: Boolean

  def asLong: java.lang.Long

  def asDouble: java.lang.Double

  def asString: String

  def asDate: Date

  def asDate(dateFormat: String): Date

  def asBytes: Array[Byte]

  def asBoolean: java.lang.Boolean

  def asBigDecimal: BigDecimal

  def asBigInteger: BigInteger

  def getByteSize: java.lang.Integer

}
