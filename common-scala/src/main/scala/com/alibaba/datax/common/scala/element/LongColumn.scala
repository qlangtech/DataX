package com.alibaba.datax.common.scala.element

import java.math.{BigDecimal, BigInteger}
import java.util.Date

import com.alibaba.datax.common.element.OverFlowUtil
import com.alibaba.datax.common.exception.{CommonErrorCode, DataXException}

case class LongColumn(val _rowData: BigInteger) extends AnyVal with Column {


  /**
   * 从整形字符串表示转为LongColumn，支持Java科学计数法
   *
   * NOTE: <br>
   * 如果data为浮点类型的字符串表示，数据将会失真，请使用DoubleColumn对接浮点字符串
   *
   **/
  //  def this(data: String) {
  //    this()
  //    if (null == data) return
  //    try {
  //      this._rowData = NumberUtils.createBigDecimal(data).toBigInteger
  //      //  super.setRawData(rawData)
  //      // 当 rawData 为[0-127]时，rawData.bitLength() < 8，导致其 byteSize = 0，简单起见，直接认为其长度为 data.length()
  //      // super.setByteSize(rawData.bitLength() / 8);
  //      // super.setByteSize(data.length)
  //    } catch {
  //      case e: Exception =>
  //        throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, String.format("String[%s]不能转为Long .", data))
  //    }
  //  }
  //
  //  def this(data: BigInteger) {
  //    this(data, if (null == data) 0
  //    else 8)
  //  }
  //
  //  def this(data: java.lang.Long) {
  //    this(if (null == data) null.asInstanceOf[BigInteger]
  //    else BigInteger.valueOf(data))
  //  }
  //
  //  def this(data: Integer) {
  //    this(if (null == data) null.asInstanceOf[BigInteger]
  //    else BigInteger.valueOf(data.longValue()))
  //  }
  //
  //  def this(data: BigInteger, byteSize: Int) {
  //    this()
  //    // super (data, Column.Type.LONG, byteSize)
  //  }

  override def asBigInteger: BigInteger = {
    if (null == this._rowData) return null
    this._rowData
  }

  override def asLong: java.lang.Long = {
    if (null == _rowData) return null
    OverFlowUtil.validateLongNotOverFlow(_rowData)
    _rowData.longValue
  }

  override def asDouble: java.lang.Double = {
    if (null == this._rowData) return null
    val decimal = this.asBigDecimal
    OverFlowUtil.validateDoubleNotOverFlow(decimal)
    decimal.doubleValue
  }

  override def asBoolean: java.lang.Boolean = {
    if (null == this._rowData) return null
    if (this.asBigInteger.compareTo(BigInteger.ZERO) != 0) true
    else false
  }

  override def asBigDecimal: BigDecimal = {
    if (null == this._rowData) return null
    new BigDecimal(this.asBigInteger)
  }

  override def asString: String = {
    if (null == this._rowData) return null
    this._rowData.toString
  }

  override def asDate: Date = {
    if (null == this._rowData) return null
    new Date(this.asLong)
  }

  override def asDate(dateFormat: String) = this.asDate

  override def asBytes = throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Long类型不能转为Bytes .")

  override def isNull: Boolean = this._rowData == null

  override def getByteSize: Integer = ???
}

object LongColumn {
  def create(value: java.lang.Long): LongColumn = {
    LongColumn(BigInteger.valueOf(value))
  }

  def create(value: java.lang.String): LongColumn = {
    LongColumn(BigInteger.valueOf(java.lang.Long.parseLong(value)))
  }
}
