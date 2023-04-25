package com.alibaba.datax.common.scala.record

import java.util

import com.alibaba.datax.common.exception.DataXException
import _root_.com.alibaba.datax.common.scala.element.{Column, Record}
import com.alibaba.datax.core.util.{ClassSize, FrameworkErrorCode, RecordUtils}
import com.alibaba.fastjson.JSON

/**
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-04-25 12:56
 **/
class DefaultRecord extends Record {
  private val RECORD_AVERGAE_COLUMN_NUMBER = 16

  private val columns = new util.ArrayList[Column](RECORD_AVERGAE_COLUMN_NUMBER)

  private var byteSize = 0

  // 首先是Record本身需要的内存
  private var memorySize = ClassSize.DefaultRecordHead


  override def addColumn(column: Column): Unit = {
    columns.add(column)
    incrByteSize(column)
  }

  override def getColumn(i: Integer): Column = {
    if (i < 0 || i >= columns.size) return null
    columns.get(i)
  }

  override def setColumn(i: Integer, column: Column): Unit = {
    if (i < 0) throw DataXException.asDataXException(FrameworkErrorCode.ARGUMENT_ERROR, "不能给index小于0的column设置值")
    if (i >= columns.size) expandCapacity(i + 1)
    decrByteSize(getColumn(i))
    this.columns.set(i, column)
    incrByteSize(getColumn(i))
  }

  override def toString: String = {
    val json = new util.HashMap[String, AnyRef]
    json.put("size", this.getColumnNumber)
    json.put("data", this.columns)
//    JSON.toJSONString(json)
    RecordUtils.toJSONString(json)
  }

  override def getColumnNumber: Integer = this.columns.size

  override def getByteSize: Integer = byteSize

  override def getMemorySize: Integer = memorySize

  private def decrByteSize(column: Column): Unit = {
    if (null == column) return
    byteSize -= column.getByteSize
    //内存的占用是column对象的头 再加实际大小
    memorySize = memorySize - ClassSize.ColumnHead - column.getByteSize
  }

  private def incrByteSize(column: Column): Unit = {
    if (null == column) return
    byteSize += column.getByteSize
    memorySize = memorySize + ClassSize.ColumnHead + column.getByteSize
  }

  private def expandCapacity(totalSize: Int): Unit = {
    if (totalSize <= 0) return
    var needToExpand = totalSize - columns.size
    while ( {
      {
        needToExpand -= 1;
        needToExpand + 1
      } > 0
    }) this.columns.add(null)
  }
}
