package com.alibaba.datax.plugin.rdbms.reader

import java.math.BigInteger
import java.sql.{ResultSet, Types}

import com.alibaba.datax.common.exception.DataXException
import com.alibaba.datax.common.plugin.{RecordSender, TaskPluginCollector}
import com.alibaba.datax.common.scala.element._
import com.alibaba.datax.core.job.IJobContainerContext
import com.alibaba.datax.plugin.rdbms.util.{DBUtilErrorCode, DataBaseType}
import com.qlangtech.tis.plugin.ds.ColumnMetaData
import org.apache.commons.lang3.StringUtils

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-05-02 13:10
 **/
object CommonRdbmsReader {

  class Task(val dataBaseType: DataBaseType, val containerContext: IJobContainerContext, val taskGropuId: java.lang.Integer, val taskId: java.lang.Integer)
    extends BaseCommonRdbmsReader.BaseTask(dataBaseType, containerContext, taskGropuId, taskId) {
    override protected def buildRecord(recordSender: RecordSender, rs: ResultSet
                                       , cols: java.util.List[ColumnMetaData], columnNumber: Int
                                       , mandatoryEncoding: String, taskPluginCollector: TaskPluginCollector): Record = {


      val record = recordSender.createRecord
      var cm: ColumnMetaData = null
      try {
        var i = 1
        while ( {
          i <= columnNumber
        }) {
          cm = cols.get(i - 1)
          cm.getType.`type` match {
            case Types.CHAR =>
            case Types.NCHAR =>
            case Types.VARCHAR =>
            case Types.LONGVARCHAR =>
            case Types.NVARCHAR =>
            case Types.LONGNVARCHAR =>
              var rawData: String = null
              if (StringUtils.isBlank(mandatoryEncoding)) rawData = rs.getString(i)
              else rawData = new String(if (rs.getBytes(i) == null) EMPTY_CHAR_ARRAY
              else rs.getBytes(i), mandatoryEncoding)
              record.addColumn(StringColumn(rawData))

            case Types.CLOB =>
            case Types.NCLOB =>
              record.addColumn(StringColumn(rs.getString(i)))
            case Types.SMALLINT =>
            case Types.TINYINT =>
            case Types.INTEGER =>
            case Types.BIGINT =>
              record.addColumn(LongColumn(BigInteger.valueOf(rs.getLong(i))))
            case Types.NUMERIC =>
            case Types.DECIMAL =>
            //                            record.addColumn(new DoubleColumn(rs.getBigDecimal(i)));
            //                            break;
            case Types.FLOAT =>
            case Types.REAL =>
            case Types.DOUBLE =>
              record.addColumn(DoubleColumn(rs.getBigDecimal(i)))
            case Types.TIME =>
              record.addColumn(TimeColumn(rs.getTime(i)))
            // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
            case Types.DATE =>
              if (cm.getType.typeName.equalsIgnoreCase("year")) record.addColumn(LongColumn(BigInteger.valueOf(rs.getLong(i))))
              else record.addColumn(DateColumn(rs.getDate(i)))

            case Types.TIMESTAMP =>
              record.addColumn(TimeStampColumn(rs.getTimestamp(i)))
            case Types.BINARY =>
            case Types.VARBINARY =>
            case Types.BLOB =>
            case Types.LONGVARBINARY =>
              record.addColumn(BytesColumn(rs.getBytes(i)))
            // warn: bit(1) -> Types.BIT 可使用BoolColumn
            // warn: bit(>1) -> Types.VARBINARY 可使用BytesColumn
            case Types.BOOLEAN =>
            case Types.BIT =>
              record.addColumn(BoolColumn(rs.getBoolean(i)))

            case Types.NULL =>
              var stringData: String = null
              if (rs.getObject(i) != null) stringData = rs.getObject(i).toString
              record.addColumn(StringColumn(stringData))
            case _ =>
              throw DataXException.asDataXException(DBUtilErrorCode.UNSUPPORTED_TYPE, String.format("您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型. 字段:[%s]. 请尝试使用数据库函数将其转换datax支持的类型 或者不同步该字段 .", cm.toString))
          }

          {
            i += 1
          }
        }
      }
      catch {
        case e: Exception =>
          taskPluginCollector.collectDirtyRecord(record, e)
          if (e.isInstanceOf[DataXException]) throw e.asInstanceOf[DataXException]
      }
      return record


    }

  }

  class CommonRdbmsReader extends BaseCommonRdbmsReader {

  }

}
