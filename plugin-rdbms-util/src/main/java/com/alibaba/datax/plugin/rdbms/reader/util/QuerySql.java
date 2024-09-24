package com.alibaba.datax.plugin.rdbms.reader.util;

import com.alibaba.datax.common.element.DataXResultPreviewOrderByCols;
import com.alibaba.datax.common.element.QueryCriteria;
import com.alibaba.datax.common.element.ThreadLocalRows;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.ITransformerBuildInfo;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.qlangtech.tis.plugin.ds.IDBReservedKeys;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-06-14 11:04
 **/
public class QuerySql {
    private final String querySql;

    private String querySqlRewriteWithWhere;

    // private final Map<String, Integer> col2Index;
    //  private final Optional<ITransformerBuildInfo> transformerBuildCfg;

    public static QuerySql from(Configuration cfg) {
        return new QuerySql(cfg.getString(Key.QUERY_SQL));//, cfg.getMap(Key.COLS_2_INDEX, Integer.class));
        //  throw new UnsupportedOperationException();
    }

    public String appendWhereCriteria(IDBReservedKeys dbReservedKeys, ThreadLocalRows attr) {


        QueryCriteria criteria = Objects.requireNonNull(attr, "attr can not be null").getQuery();
        DataXResultPreviewOrderByCols pagerOffsetPointCols = attr.getPagerOffsetPointCols();
        if (pagerOffsetPointCols != null) {
            return setQuerySqlRewriteWithWhere(querySql
                    + pagerOffsetPointCols.createWhereAndOrderByStatment(
                    criteria.isNextPakge(), Objects.requireNonNull(dbReservedKeys, "dbReservedKeys can not be null")));
        }

        throw new IllegalStateException("has not be rewrite with where criteria");
    }

    private String setQuerySqlRewriteWithWhere(String querySqlRewriteWithWhere) {
//        if (this.shallNotRewirte) {
//            return this.querySql;
//        } else {
        return this.querySqlRewriteWithWhere = querySqlRewriteWithWhere;

    }

    public String getQuerySqlRewriteWithWhere() {
        return querySqlRewriteWithWhere;
    }

    public QuerySql(String querySql) {
        if (StringUtils.isEmpty(querySql)) {
            throw new IllegalArgumentException("param querySql can not be null");
        }
        this.querySql = querySql;
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
        return StringUtils.defaultString(this.querySqlRewriteWithWhere, this.querySql);
    }
}
