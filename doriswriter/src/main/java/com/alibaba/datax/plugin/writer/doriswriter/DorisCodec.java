package com.alibaba.datax.plugin.writer.doriswriter;

import com.alibaba.datax.common.scala.element.Record;

import java.io.Serializable;

public interface DorisCodec extends Serializable {

    String codec(Record row);
}
