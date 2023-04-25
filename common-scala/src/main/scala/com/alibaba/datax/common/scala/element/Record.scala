package com.alibaba.datax.common.scala.element


/**
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-04-25 12:52
 **/
trait Record {
  def addColumn(column: Column): Unit

  def setColumn(i: Integer, column: Column): Unit

  def getColumn(i: Integer): Column

  def getColumnNumber: Integer

  def getByteSize: Integer

  def getMemorySize: Integer
}
