package com.alibaba.datax.common.scala.element

import java.math.{BigDecimal, BigInteger}

import com.alibaba.datax.common.element.{Column, OverFlowUtil}
import com.alibaba.datax.common.exception.{CommonErrorCode, DataXException}

case class DoubleColumn(val data: BigDecimal) extends AnyVal with Column {


  //  /**
  //   * Double无法表示准确的小数数据，我们不推荐使用该方法保存Double数据，建议使用String作为构造入参
  //   **/
  //  def this(data: Double) {
  //    this(if (data == null) null.asInstanceOf[String]
  //    else new BigDecimal(String.valueOf(data)).toPlainString)
  //  }
  //
  //  /**
  //   * Float无法表示准确的小数数据，我们不推荐使用该方法保存Float数据，建议使用String作为构造入参
  //   *
  //   **/
  //  def this(data: Float) {
  //    this(if (data == null) null.asInstanceOf[String]
  //    else new BigDecimal(String.valueOf(data)).toPlainString)
  //  }
  //
  //  def this(data: BigDecimal) {
  //    this(if (null == data) null.asInstanceOf[String]
  //    else data.toPlainString)
  //  }
  //
  //  def this(data: BigInteger) {
  //    this(if (null == data) null.asInstanceOf[String]
  //    else data.toString)
  //  }
  //
  //  def this {
  //    this(null.asInstanceOf[String])
  //  }

  override def getType(): Column.Type = Column.Type.DOUBLE

  override def asBigDecimal: BigDecimal = {
    if (null == this.data) return null
    this.data;
    //    try new BigDecimal(this.getRawData.asInstanceOf[String])
    //    catch {
    //      case e: NumberFormatException =>
    //        throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, String.format("String[%s] 无法转换为Double类型 .", this.getRawData.asInstanceOf[String]))
    //    }
  }

  override def asDouble: java.lang.Double = {
    if (null == this.data) return null
//    val string = this.getRawData.asInstanceOf[String]
//    val isDoubleSpecific = string == "NaN" || string == "-Infinity" || string == "+Infinity"
//    if (isDoubleSpecific) return Double.valueOf(string)
    val result = this.data;
    OverFlowUtil.validateDoubleNotOverFlow(result)
    result.doubleValue
  }

  override def asLong: java.lang.Long = {
    if (null == this.data) return null
    val result = data;
    OverFlowUtil.validateLongNotOverFlow(result.toBigInteger)
    result.longValue
  }

  override def asBigInteger: BigInteger = {
    if (null == this.data) return null
    this.data.toBigInteger
  }

  override def asString: String = {
    if (null == this.data) return null
    this.data.toString
  }

  override def asBoolean = throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Double类型无法转为Bool .")

  override def asDate = throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Double类型无法转为Date类型 .")

  override def asDate(dateFormat: String) = throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Double类型无法转为Date类型 .")

  override def asBytes = throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Double类型无法转为Bytes类型 .")

  private def validate(data: String): Unit = {
    if (null == data) return
    if (data.equalsIgnoreCase("NaN") || data.equalsIgnoreCase("-Infinity") || data.equalsIgnoreCase("Infinity")) return
    try new BigDecimal(data)
    catch {
      case e: Exception =>
        throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, String.format("String[%s]无法转为Double类型 .", data))
    }
  }

  override def isNull: Boolean = this.data == null

  override def getByteSize: Integer = 0
}
