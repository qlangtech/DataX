package com.alibaba.datax.plugin.writer.elasticsearchwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/10/5
 */
public class DataConvertUtils {
    public static String getDateStr(ESColumn esColumn, Column column) {
        DateTime date = null;
        DateTimeZone dtz = DateTimeZone.getDefault();
        if (esColumn.getTimezone() != null) {
            // 所有时区参考 http://www.joda.org/joda-time/timezones.html
            dtz = DateTimeZone.forID(esColumn.getTimezone());
        }
        if (column.getType() != Column.Type.DATE && esColumn.getFormat() != null) {
            DateTimeFormatter formatter = DateTimeFormat.forPattern(esColumn.getFormat());
            date = formatter.withZone(dtz).parseDateTime(column.asString());
            return date.toString();
        } else if (column.getType() == Column.Type.DATE) {
            date = new DateTime(column.asLong(), dtz);
            return date.toString();
        } else {
            return column.asString();
        }
    }

    /**
     * 测试环境中使用
     */
    public static Consumer<String> esMappingConsumer = (mapping) -> {
    };

    /**
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/explicit-mapping.html
     *
     * @param typeName
     * @return
     */
    public static String genMappings(JSONArray column, String typeName, Consumer<List<ESColumn>> colsConsumer) {
        String mappings = null;
        Map<String, Object> propMap = new HashMap<String, Object>();
        List<ESColumn> columnList = new ArrayList<ESColumn>();

        //   JSONArray column = (JSONArray) conf.getList("column");
        if (column != null) {
            for (Object col : column) {
                JSONObject jo = (JSONObject) col;
                String colName = jo.getString("name");
                String colTypeStr = jo.getString("type");
                boolean pk = jo.getBooleanValue("pk");
                if (colTypeStr == null) {
                    //throw DataXException.asDataXException(ESWriterErrorCode.BAD_CONFIG_VALUE, col.toString() + "
                    // column must have type");
                    throw new IllegalStateException(col.toString() + " column must have type");
                }
                ESFieldType colType = ESFieldType.getESFieldType(colTypeStr);
                if (colType == null) {
                    // throw DataXException.asDataXException(ESWriterErrorCode.BAD_CONFIG_VALUE, col.toString() + "
                    // unsupported type");
                    throw new IllegalStateException(col.toString() + " unsupported type");
                }

                ESColumn columnItem = new ESColumn();

//                if (colName.equals(Key.PRIMARY_KEY_COLUMN_NAME)) {
//                    // 兼容已有版本
//                    colType = ESFieldType.ID;
//                    colTypeStr = "id";
//                }

                columnItem.setName(colName);
                columnItem.setType(colTypeStr);
                columnItem.setPk(pk);

//                if (pk) {
//                    // https://www.elastic.co/guide/en/elasticsearch/reference/current/copy-to.html
//                    Map<String, Object> idField = new HashMap<>();
//                    idField.put("type", "id");
//                    propMap.put("id", idField);
//                }

//                if (colType == ESFieldType.ID) {
//                    columnList.add(columnItem);
//                    // 如果是id,则properties为空
//                    continue;
//                }

                Boolean array = jo.getBoolean("array");
                if (array != null) {
                    columnItem.setArray(array);
                }
                Map<String, Object> field = new HashMap<String, Object>();
                field.put("type", colTypeStr);
                //https://www.elastic.co/guide/en/elasticsearch/reference/5.2/breaking_50_mapping_changes.html#_literal_index_literal_property
                // https://www.elastic.co/guide/en/elasticsearch/guide/2.x/_deep_dive_on_doc_values.html#_disabling_doc_values
                field.put("doc_values", jo.getBoolean("doc_values"));
                field.put("ignore_above", jo.getInteger("ignore_above"));
                field.put("index", jo.getBoolean("index"));

                switch (colType) {
                    //                    case STRING:
                    //                        // 兼容string类型,ES5之前版本
                    //                        break;
                    case KEYWORD:
                        // https://www.elastic.co/guide/en/elasticsearch/reference/current/tune-for-search-speed.html#_warm_up_global_ordinals
                        Boolean eagerGlobalOrdinals = jo.getBoolean("eager_global_ordinals");
                        if (eagerGlobalOrdinals != null) {
                            field.put("eager_global_ordinals", eagerGlobalOrdinals);
                        }
                        break;
                    case TEXT:
                        field.put("analyzer", jo.getString("analyzer"));
                        // 优化disk使用,也同步会提高index性能
                        // https://www.elastic.co/guide/en/elasticsearch/reference/current/tune-for-disk-usage.html
                        field.put("norms", jo.getBoolean("norms"));
                        field.put("index_options", jo.getBoolean("index_options"));
                        break;
                    case DATE:
                        columnItem.setTimeZone(jo.getString("timezone"));
                        columnItem.setFormat(jo.getString("format"));
                        // 后面时间会处理为带时区的标准时间,所以不需要给ES指定格式
                            /*
                            if (jo.getString("format") != null) {
                                field.put("format", jo.getString("format"));
                            } else {
                                //field.put("format", "strict_date_optional_time||epoch_millis||yyyy-MM-dd
                                HH:mm:ss||yyyy-MM-dd");
                            }
                            */
                        break;
                    case GEO_SHAPE:
                        field.put("tree", jo.getString("tree"));
                        field.put("precision", jo.getString("precision"));
                    default:
                        break;
                }
                propMap.put(colName, field);
                columnList.add(columnItem);
            }
        } else {
            throw new IllegalStateException("conf.getList(\"column\") can not be empty");
        }

        colsConsumer.accept(columnList);
        // conf.set(WRITE_COLUMNS, JSON.toJSONString(columnList));


        Map<String, Object> rootMappings = new HashMap<String, Object>();
        Map<String, Object> typeMappings = new HashMap<String, Object>();
        typeMappings.put("properties", propMap);
        rootMappings.put(typeName, typeMappings);

        mappings = StringUtils.isNotBlank(typeName) ? JSON.toJSONString(rootMappings) : JSON.toJSONString(typeMappings);

        if (StringUtils.isEmpty(mappings)) {
            //throw DataXException.asDataXException(ESWriterErrorCode.BAD_CONFIG_VALUE, "must have mappings");
            throw new IllegalStateException("must have mappings");
        }
        //ESClient.log.info(mappings);
        esMappingConsumer.accept(mappings);
        return mappings;
    }
}
