package com.alibaba.datax.plugin.rdbms.reader.util;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.ICol2Index;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.core.job.ITransformerBuildInfo;
import com.google.common.collect.Maps;
import com.qlangtech.tis.plugin.datax.transformer.OutputParameter;
import com.qlangtech.tis.plugin.ds.ContextParamConfig;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.DataType.TypeVisitor;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugin.ds.RunningContext;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-07-03 13:55
 **/
public class DataXCol2Index implements ICol2Index {
    private final Map<String, ColumnBiFunction> mapper;

    private final Map<String, Object> contextParamVals;
    final int contextParamValsCount;
    private final List<OutputParameter> outputVirtualParameters;
    private Map<String, ICol2Index.Col> key2Idx;

    @Override
    public int contextParamValsCount() {
        return this.contextParamValsCount;
    }

    /**
     * @param mapper
     * @param contextParamVals 执行上下文绑定参数
     */
    public DataXCol2Index(Map<String, ColumnBiFunction> mapper
            , Map<String, Object> contextParamVals
            , List<OutputParameter> outputParameters) {
        this.mapper = mapper;
        this.contextParamVals = Objects.requireNonNull(contextParamVals, "contextParamVals can not be null");
        this.contextParamValsCount = this.contextParamVals.size();
        this.outputVirtualParameters = Objects.requireNonNull(outputParameters, "outputParameters can not be null").stream()
                .filter((param) -> param.isVirtual()).collect(Collectors.toUnmodifiableList());
    }

    public Map<String, Object> getContextParamVals() {
        return this.contextParamVals;
    }

    /**
     * 填充记录
     *
     * @param record
     * @return
     */
    public Record fill(Record record) {
        record.setCol2Index(this);
        for (Map.Entry<String, Object> entry : contextParamVals.entrySet()) {
            record.setColumn(entry.getKey(), entry.getValue());
        }

        for (OutputParameter outParam : this.outputVirtualParameters) {
            ICol2Index.Col colIndex = getCol2Index().get(outParam.getName());
            if (colIndex == null) {
                throw new NullPointerException("param key:" + outParam.getName() + " relevant colIndex can not be null");
            }
            record.setColumn(colIndex.getIndex(), Column.NULL);
        }
        return record;
    }

    @Override
    public Map<String, ICol2Index.Col> getCol2Index() {
        if (key2Idx == null) {
            key2Idx = mapper.entrySet().stream().collect(Collectors.toMap((e) -> e.getKey() //
                    , (e) -> new ICol2Index.Col(e.getValue().getColumnIndex(), e.getValue().getType())));
        }
        return key2Idx;

    }

    public static <T extends IColMetaGetter> DataXCol2Index getCol2Index(
            Optional<ITransformerBuildInfo> transformerBuildCfg, RunningContext runningContext, List<T> sourceCols) {
        //  return this.col2Index;
        AtomicInteger idx = new AtomicInteger();
        Map<String, ColumnBiFunction> result = transformerBuildCfg.map((transformer) -> {
                    List<OutputParameter> overwriteCols = transformer.overwriteColsWithContextParams(sourceCols);
                    return overwriteCols.stream().map((c) -> (IColMetaGetter) c);
                }).orElseGet(() -> sourceCols.stream().map((c) -> c))
                .collect(
                        Collectors.toMap((colGetter) -> colGetter.getName(), (colGetter) -> {
                            {
                                return colGetter.getType().accept(new TypeVisitor<ColumnBiFunction>() {
                                    @Override
                                    public ColumnBiFunction bigInt(DataType type) {
                                        return new ColumnBiFunction(type, idx.getAndIncrement()) {
                                            @Override
                                            public Column toColumn(Object val) {
                                                if (val instanceof Long) {
                                                    return new LongColumn((Long) (val));
                                                } else if (val instanceof Integer) {
                                                    return new LongColumn((Integer) (val));
                                                } else if (val instanceof BigInteger) {
                                                    return new LongColumn((BigInteger) (val));
                                                } else {
                                                    return new LongColumn(String.valueOf(val));
                                                }
                                            }

                                            @Override
                                            public Object toInternal(Column col) {
                                                return col.asLong();
                                            }
                                        };
                                    }

                                    @Override
                                    public ColumnBiFunction doubleType(DataType type) {
                                        return new ColumnBiFunction(type, idx.getAndIncrement()) {
                                            @Override
                                            public Column toColumn(Object val) {
                                                if (val instanceof Long) {
                                                    return new DoubleColumn((Long) val);
                                                } else if (val instanceof Integer) {
                                                    return new DoubleColumn((Integer) val);
                                                } else if (val instanceof Double) {
                                                    return new DoubleColumn((Double) val);
                                                } else if (val instanceof Float) {
                                                    return new DoubleColumn((Float) val);
                                                } else if (val instanceof BigDecimal) {
                                                    return new DoubleColumn((BigDecimal) val);
                                                } else if (val instanceof BigInteger) {
                                                    return new DoubleColumn((BigInteger) val);
                                                } else {
                                                    return new DoubleColumn(String.valueOf(val));
                                                }
                                            }

                                            @Override
                                            public Object toInternal(Column col) {
                                                return col.asDouble();
                                            }
                                        };
                                    }

                                    @Override
                                    public ColumnBiFunction dateType(DataType type) {
                                        return new ColumnBiFunction(type, idx.getAndIncrement()) {
                                            @Override
                                            public Column toColumn(Object val) {
                                                if (val instanceof Long) {
                                                    return new DateColumn((Long) val);
                                                } else if (val instanceof java.sql.Date) {
                                                    return new DateColumn((java.sql.Date) val);
                                                } else if (val instanceof java.sql.Time) {
                                                    return new DateColumn((java.sql.Time) val);
                                                } else if (val instanceof java.sql.Timestamp) {
                                                    return new DateColumn((java.sql.Timestamp) val);
                                                } else if (val instanceof Date) {
                                                    return new DateColumn((Date) val);
                                                } else {
                                                    throw new IllegalStateException("illegal type:" + val.getClass() + ",val:" + String.valueOf(val));
                                                }
                                            }

                                            @Override
                                            public Object toInternal(Column col) {
                                                return col.asDate();
                                            }
                                        };
                                    }

                                    @Override
                                    public ColumnBiFunction timestampType(DataType type) {
                                        return this.dateType(type);
                                    }

                                    @Override
                                    public ColumnBiFunction bitType(DataType type) {
                                        return new ColumnBiFunction(type, idx.getAndIncrement()) {
                                            @Override
                                            public Column toColumn(Object val) {
                                                if (val instanceof Number) {
                                                    return new BoolColumn(((Number) val).intValue() > 0);
                                                } else if (val instanceof Boolean) {
                                                    return new BoolColumn((Boolean) val);
                                                } else {
                                                    return new BoolColumn(String.valueOf(val));
                                                }
                                            }

                                            @Override
                                            public Object toInternal(Column col) {
                                                return col.asBoolean();
                                            }
                                        };
                                    }

                                    @Override
                                    public ColumnBiFunction blobType(DataType type) {
                                        return new ColumnBiFunction(type, idx.getAndIncrement()) {
                                            @Override
                                            public Column toColumn(Object val) {
                                                return new BytesColumn((byte[]) val);
                                            }

                                            @Override
                                            public Object toInternal(Column col) {
                                                return col.asBytes();
                                            }
                                        };
                                    }

                                    @Override
                                    public ColumnBiFunction varcharType(DataType type) {
                                        return new ColumnBiFunction(type, idx.getAndIncrement()) {
                                            @Override
                                            public Column toColumn(Object val) {
                                                return new StringColumn(String.valueOf(val));
                                            }

                                            @Override
                                            public Object toInternal(Column col) {
                                                return col.asString();
                                            }
                                        };
                                    }
                                });
                            }
                        }));

        Map<String, Object> contextParamVals = Maps.newHashMap();
        ITransformerBuildInfo transformerBuildInfo = null;
        List<OutputParameter> outputParameters = Collections.emptyList();
        if (transformerBuildCfg.isPresent()
                && (transformerBuildInfo = transformerBuildCfg.get()).containContextParams()) {
            contextParamVals = transformerBuildInfo.contextParamVals(runningContext);
        }
        if (transformerBuildCfg.isPresent()) {
            outputParameters = transformerBuildCfg.get().tranformerColsWithoutContextParams();
        }
        return new DataXCol2Index(result, contextParamVals, outputParameters);
    }

    public ColumnBiFunction get(String field) {
        ColumnBiFunction func = mapper.get(field);
        if (func == null) {
            throw new IllegalStateException("field:" + field + " relevant func can not be null,cols:"
                    + String.join(",", this.mapper.keySet()));
        }
        return func;
    }
}
