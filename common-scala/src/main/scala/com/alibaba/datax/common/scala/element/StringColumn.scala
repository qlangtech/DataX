package com.alibaba.datax.common.scala.element

import java.math.{BigDecimal, BigInteger}
import java.text.ParseException
import java.util.Date

import com.alibaba.datax.common.element.OverFlowUtil
import com.alibaba.datax.common.exception.{CommonErrorCode, DataXException}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.FastDateFormat

/**
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-04-25 12:55
 **/
case class StringColumn(colVal: String) extends AnyVal with Column {

  override def isNull: Boolean = StringUtils.isEmpty(this.colVal)

  override def asLong: java.lang.Long = {
    if (null == colVal) return null

    this.validateDoubleSpecific(colVal)

    try {
      val integer = this.asBigInteger
      OverFlowUtil.validateLongNotOverFlow(integer)
      return integer.longValue
    } catch {
      case e: Exception =>
        throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, String.format("String[\"%s\"]不能转为Long .", this.asString))
    }
  }

  override def getByteSize: Integer = 0

  override def asDouble: java.lang.Double = {
    if (null == this.colVal) return null

    val data = colVal
    if ("NaN" == data) return Double.NaN

    if ("Infinity" == data) return Double.PositiveInfinity

    if ("-Infinity" == data) return Double.NegativeInfinity

    val decimal = this.asBigDecimal
    OverFlowUtil.validateDoubleNotOverFlow(decimal)

    return decimal.doubleValue
  }

  override def asString: String = colVal

  override def asDate: Date = {
    if (null == colVal) return null


    try return StringCast.datetimeFormatter.parse(colVal)
    catch {
      case e: ParseException =>
    }


    try return StringCast.dateFormatter.parse(colVal)
    catch {
      case e: ParseException =>
    }

    var e: ParseException = null
    try return StringCast.timeFormatter.parse(colVal)
    catch {
      case ignored: ParseException =>
        e = ignored
    }

    import scala.collection.JavaConversions._
    for (format <- StringCast.extraFormats) {
      return FastDateFormat.getInstance(format, StringCast.timeZoner).parse(colVal)
    }

    throw new IllegalStateException
  }

  override def asDate(dateFormat: String): Date
  = FastDateFormat.getInstance(dateFormat, StringCast.timeZoner).parse(colVal)


  override def asBytes: Array[Byte] = try return colVal.getBytes(StringCast.encoding)
  catch {
    case e: Exception =>
      throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, String.format("String[\"%s\"]不能转为Bytes .", this.asString))
  }

  override def asBoolean: java.lang.Boolean = {
    if (null == this.colVal) return null

    if ("true".equalsIgnoreCase(this.asString)) return true

    if ("false".equalsIgnoreCase(this.asString)) return false

    throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, String.format("String[\"%s\"]不能转为Bool .", this.asString))
  }

  override def asBigDecimal: BigDecimal = {
    if (null == this.colVal) return null

    this.validateDoubleSpecific(this.colVal)

    try return new BigDecimal(this.asString)
    catch {
      case e: Exception =>
        throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, String.format("String [\"%s\"] 不能转为BigDecimal .", this.asString))
    }
  }

  override def asBigInteger: BigInteger = {
    if (null == this.colVal) return null

    this.validateDoubleSpecific(this.colVal)

    try return this.asBigDecimal.toBigInteger
    catch {
      case e: Exception =>
        throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, String.format("String[\"%s\"]不能转为BigInteger .", this.asString))
    }
  }


  private def validateDoubleSpecific(data: String): Unit = {
    if ("NaN" == data || "Infinity" == data || "-Infinity" == data)
      throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, String.format("String[\"%s\"]属于Double特殊类型，不能转为其他类型 .", data))
  }
}
