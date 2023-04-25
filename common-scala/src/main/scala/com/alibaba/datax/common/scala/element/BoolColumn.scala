package com.alibaba.datax.common.scala.element

import java.math.{BigDecimal, BigInteger}

import com.alibaba.datax.common.exception.{CommonErrorCode, DataXException}

/**
 * Created by jingxing on 14-8-24.
 */
class BoolColumn(val bool: java.lang.Boolean) extends AnyVal with Column {
//  def this(bool: Boolean) {
//    this()
//    super (bool, Column.Type.BOOL, 1)
//  }
//
//  def this(data: String) {
//    this(true)
//    this.validate(data)
//    if (null == data) {
//      this.setRawData(null)
//      this.setByteSize(0)
//    }
//    else {
//      this.setRawData(Boolean.valueOf(data))
//      this.setByteSize(1)
//    }
//  }

  override def asBoolean: java.lang.Boolean = {
    if (null == bool) return null
    bool
  }

  override def asLong: java.lang.Long = {
    if (null == this.bool) return null
    if (this.asBoolean) 1L
    else 0L
  }

  override def asDouble: java.lang.Double = {
    if (null == this.bool) return null
    if (this.asBoolean) 1.0d
    else 0.0d
  }

  override def asString: String = {
    if (null == bool) return null
    if (this.asBoolean) "true"
    else "false"
  }

  override def asBigInteger: BigInteger = {
    if (null == this.bool) return null
    BigInteger.valueOf(this.asLong)
  }

  override def asBigDecimal: BigDecimal = {
    if (null == this.bool) return null
    BigDecimal.valueOf(this.asLong)
  }

  override def asDate = throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Bool类型不能转为Date .")

  override def asDate(dateFormat: String) = throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Bool类型不能转为Date .")

  override def asBytes = throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Boolean类型不能转为Bytes .")

  private def validate(data: String): Unit = {
    if (null == data) return
    if ("true".equalsIgnoreCase(data) || "false".equalsIgnoreCase(data)) return
    throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, String.format("String[%s]不能转为Bool .", data))
  }

  override def isNull: Boolean = this.bool == null

  override def getByteSize: Integer = ???
}
