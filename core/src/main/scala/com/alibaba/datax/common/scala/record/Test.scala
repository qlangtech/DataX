package com.alibaba.datax.common.scala.record

import com.alibaba.datax.common.scala.element.StringColumn

/**
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-05-04 19:32
 **/
object Test {
  def main(args: Array[String]): Unit = {
    val s = StringColumn("test");
    println(s.asString)
  }
}
