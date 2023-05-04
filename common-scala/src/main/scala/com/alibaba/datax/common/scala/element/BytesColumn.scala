package com.alibaba.datax.common.scala.element

import com.alibaba.datax.common.element.ColumnCast
import com.alibaba.datax.common.exception.{CommonErrorCode, DataXException}

/**
 * Created by jingxing on 14-8-24.
 */
case class BytesColumn(val bytes: Array[Byte]) extends AnyVal with Column {

  override def asBytes: Array[Byte] = {
    if (null == this.bytes) return null
    bytes
  }

  override def asString: String = {
    if (null == this.bytes) return null
    try ColumnCast.bytes2String(this)
    catch {
      case e: Exception =>
        throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, String.format("Bytes[%s]不能转为String .", this.toString))
    }
  }

  override def asLong = throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为Long .")

  override def asBigDecimal = throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为BigDecimal .")

  override def asBigInteger = throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为BigInteger .")

  override def asDouble = throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为Long .")

  override def asDate = throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为Date .")

  override def asDate(dateFormat: String) = throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为Date .")

  override def asBoolean = throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为Boolean .")

  override def isNull: Boolean = this.bytes == null

  override def getByteSize: Integer = 0
}
