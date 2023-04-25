package com.alibaba.datax.core.transport.record

import com.alibaba.datax.common.scala.element.{Column, Record}

//import com.alibaba.datax.common.element.Column
//import com.alibaba.datax.common.element.Record

/**
 * 作为标示 生产者已经完成生产的标志
 *
 */
object TerminateRecord {
  private val SINGLE = new TerminateRecord

  def get = SINGLE
}

class TerminateRecord private() extends Record {
  override def addColumn(column: Column): Unit = ???

  override def setColumn(i: Integer, column: Column): Unit = ???

  override def getColumn(i: Integer): Column = ???

  override def getColumnNumber: Integer = ???

  override def getByteSize: Integer = ???

  override def getMemorySize: Integer = ???
}
