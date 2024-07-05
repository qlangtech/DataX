package com.alibaba.datax.plugin.rdbms.reader.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.ITransformerBuildInfo;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-06-14 11:04
 **/
public class QuerySql {
    private final String querySql;
    // private final Map<String, Integer> col2Index;
    //  private final Optional<ITransformerBuildInfo> transformerBuildCfg;

    public static QuerySql from(Configuration cfg) {
        return new QuerySql(cfg.getString(Key.QUERY_SQL));//, cfg.getMap(Key.COLS_2_INDEX, Integer.class));
        //  throw new UnsupportedOperationException();
    }

//    public QuerySql(String querySql, List<String> cols, Optional<ITransformerBuildInfo> transformerBuildCfg) {
//        this(querySql, transformerBuildCfg //toMap(cols, transformerBuildCfg)
//        );
//    }

    public QuerySql(String querySql //, Optional<ITransformerBuildInfo> transformerBuildCfg
    ) {
        if (StringUtils.isEmpty(querySql)) {
            throw new IllegalArgumentException("param querySql can not be null");
        }
        this.querySql = querySql;
        //  this.transformerBuildCfg = transformerBuildCfg;
        //  this.col2Index = Objects.requireNonNull(col2Index, "param col2Idx can not be null");
    }

    private static Map<String, Integer> toMap(List<String> cols, Optional<ITransformerBuildInfo> transformerBuildCfg) {
//        if (CollectionUtils.isEmpty(cols)) {
//            throw new IllegalArgumentException("param cols can not be empty");
//        }
//        Builder<String, Integer> mapBuilder = ImmutableMap.builder();
//        Set<String> added = Sets.newHashSet();
//        String key = null;
//        int idx = 0;
//        for (; idx < cols.size(); idx++) {
//            key = cols.get(idx);
//            mapBuilder.put(key, idx);
//            added.add(key);
//        }
//
//        if (transformerBuildCfg.isPresent()) {
//            for (String transformerOutterKey : transformerBuildCfg.get().relevantOutterColKeys()) {
//                if (!added.contains(transformerOutterKey)) {
//                    mapBuilder.put(transformerOutterKey, idx++);
//                }
//            }
//        }
//
//        return mapBuilder.build();
        throw new UnsupportedOperationException();
    }


    public String getQuerySql() {
        return this.querySql;
    }

    public void write(Configuration cfg) {
        cfg.set(Key.QUERY_SQL, this.getQuerySql());
        // cfg.set(Key.COLS_2_INDEX, this.getCol2Index());
    }

    @Override
    public String toString() {
        return this.querySql;
    }
}
