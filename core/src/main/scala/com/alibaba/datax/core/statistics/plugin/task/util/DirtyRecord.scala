package com.alibaba.datax.core.statistics.plugin.task.util

//import com.alibaba.datax.common.element.Column;
//import com.alibaba.datax.common.element.Record;

import java.util

import com.alibaba.datax.common.exception.DataXException
import com.alibaba.datax.common.scala.element.{Column, Record}
import com.alibaba.datax.core.util.{FrameworkErrorCode, RecordUtils}
import com.alibaba.fastjson.JSON

object DirtyRecord {
  def asDirtyRecord(record: Record) = {
    val result = new DirtyRecord
    var i = 0
    while ( {
      i < record.getColumnNumber
    }) {
      result.addColumn(record.getColumn(i))
      i = i - 1
    }
    result
  }
}

class DirtyRecord extends Record {


  private var columns: util.List[Column] = new util.ArrayList[Column]

  def addColumn(column: Column) = this.columns.add(DirtyColumn.asDirtyColumn(column, this.columns.size))

  override def toString = RecordUtils.toJSONString(this.columns)

  def setColumn(i: Int, column: Column) = throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, "该方法不支持!")

  def getColumn(i: Int) = throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, "该方法不支持!")

  def getColumnNumber = throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, "该方法不支持!")

  def getByteSize = throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, "该方法不支持!")

  def getMemorySize = throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, "该方法不支持!")

  def getColumns = columns

  def setColumns(columns: util.List[Column]) = this.columns = columns

  override def setColumn(i: Integer, column: Column): Unit = ???

  override def getColumn(i: Integer): Column = ???
}

object DirtyColumn {
  def asDirtyColumn(column: Column, index: Integer) = new DirtyColumn(column, index)
}

class DirtyColumn(`val`: Any, var index: Integer) extends Column {
  //this.setIndex(index)


  //  def this(column: Column, index: Int) {
  //    this(if (null == column) null
  //    else column.getRawData, if (null == column) Column.Type.NULL
  //    else column.getType, if (null == column) 0
  //    else column.getByteSize, index)
  //  }

  def getIndex = this.index

  def setIndex(index: Integer) = this.index = index

  override def asLong = throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, "该方法不支持!")

  override def asDouble = throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, "该方法不支持!")

  override def asString = throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, "该方法不支持!")

  override def asDate = throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, "该方法不支持!")

  override def asDate(dateFormat: String) = throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, "该方法不支持!")

  override def asBytes = throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, "该方法不支持!")

  override def asBoolean = throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, "该方法不支持!")

  override def asBigDecimal = throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, "该方法不支持!")

  override def asBigInteger = throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, "该方法不支持!")

  override def isNull: Boolean = ???

  override def getByteSize: Integer = ???
}
