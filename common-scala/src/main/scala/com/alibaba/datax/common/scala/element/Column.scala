package com.alibaba.datax.common.scala.element

import java.math.{BigDecimal, BigInteger}
import java.util.Date

/**
 *
 */
trait Column extends Any {
  def isNull: Boolean

  def getType(): Column.Type.Type;

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

//object Column {
//
//  object Type extends Enumeration {
//    type Type = Value
//    val BAD = Type(1)
//    val NULL = Type(2)
//    val INT = Type(3)
//    val LONG = Type(4)
//    val DOUBLE = Type(5)
//    val STRING = Type(6)
//    val BOOL = Type(7)
//    val DATE = Type(8)
//    val BYTES = Type(9)
//  }
//
//}
