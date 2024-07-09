package com.alibaba.datax.plugin.writer.elasticsearchwriter;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.searchbox.action.Action;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.aliases.*;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.indices.mapping.GetMapping.Builder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by xiongfeng.bxf on 17/2/8.
 */
public class ESClient {
    private static final Logger log = LoggerFactory.getLogger(ESClient.class);
    final ESInitialization es;

    public ESClient(ESInitialization es) {
        this.es = es;
    }


    public boolean indicesExists(String indexName) throws Exception {
        //  boolean isIndicesExists = false;
        JestResult rst = es.jestClient.execute(new IndicesExists.Builder(indexName).build());
        return parseResponse(rst, new ParseResponse<Boolean>() {
            @Override
            public Boolean success(JestResult rst) {
                return true;
            }

            @Override
            public Boolean faild() {
                return false;
            }
        });

//        if (rst.isSucceeded()) {
//            isIndicesExists = true;
//        } else {
//            switch (rst.getResponseCode()) {
//                case 404:
//                    isIndicesExists = false;
//                    break;
//                case 401:
//                    // 无权访问
//                default:
//                    log.warn(rst.getErrorMessage());
//                    break;
//            }
//        }
//        return isIndicesExists;
    }

    private <T> T parseResponse(JestResult rst, ParseResponse<T> parseResponse) {
        if (rst.isSucceeded()) {
            return parseResponse.success(rst);
        } else {
            switch (rst.getResponseCode()) {
                case 404:
                    return parseResponse.faild();
                case 401:
                    // 无权访问
                default:
                    log.warn(rst.getErrorMessage());
                    break;
            }
        }

        return parseResponse.faild();
    }

    interface ParseResponse<T> {
        T success(JestResult rst);

        T faild();
    }

    public static class SchemaCol {
        private final String name;
        private final String type;

        public SchemaCol(String name, String type) {
            this.name = name;
            this.type = type;
        }

        @Override
        public String toString() {
            return "SchemaCol{" +
                    "name='" + name + '\'' +
                    ", type='" + type + '\'' +
                    '}';
        }
    }

    /**
     * 取得index的Schema mapping
     *
     * @param indexName
     * @return
     * @throws Exception
     */
    public List<SchemaCol> getMapping(String indexName) throws Exception {
        GetMapping.Builder builder = new GetMapping.Builder();
        builder.addIndex(indexName);
        JestResult rst = es.jestClient.execute(builder.build());

        return parseResponse(rst, new ParseResponse<List<SchemaCol>>() {
            @Override
            public List<SchemaCol> success(JestResult rst) {
                JsonObject obj = rst.getJsonObject();
                JsonObject mapping = obj.getAsJsonObject(indexName);
                JsonObject props = Objects.requireNonNull(mapping.getAsJsonObject("mappings"), "mapping can not be null")
                        .getAsJsonObject("properties");
                JsonObject typeMeta = null;
                List<SchemaCol> parseMapping = Lists.newArrayList();
                SchemaCol col = null;
                for (Map.Entry<String, JsonElement> entry : Objects.requireNonNull(props, "props can not be null").entrySet()) {

                    typeMeta = entry.getValue().getAsJsonObject();

                    col = new SchemaCol(entry.getKey(), typeMeta.get("type").getAsString());
                    parseMapping.add(col);
                }
                return parseMapping;
            }

            @Override
            public List<SchemaCol> faild() {
                return Collections.emptyList();
            }
        });

        //  rst.getSourceAsObject()
    }

    public boolean deleteIndex(String indexName) throws Exception {
        log.info("delete index " + indexName);
        if (indicesExists(indexName)) {
            JestResult rst = execute(new DeleteIndex.Builder(indexName).build());
            if (!rst.isSucceeded()) {
                return false;
            }
        } else {
            log.info("index cannot found, skip delete " + indexName);
        }
        return true;
    }

    public boolean createIndex(String indexName, String typeName
            , Pair<String, List<ESColumn>> mappings
            , String settings, boolean dynamic) throws Exception {
        JestResult rst = null;
        if (!indicesExists(indexName)) {
            log.info("create index " + indexName);
            rst = es.jestClient.execute(new CreateIndex.Builder(indexName)
                    .settings(settings)
                    .mappings((mappings.getKey()))
                    .setParameter("master_timeout", "5m").build());
            //index_already_exists_exception
            if (!rst.isSucceeded()) {
                throw new IllegalStateException("createIndex faild:" + rst.getErrorMessage());
            } else {
                log.info(String.format("create [%s] index success", indexName));
                log.info(mappings.getKey());
                return true;
            }
        } else {
            //
            ESColumn cfg = null;
            //  SchemaCol ecfg = null;
            List<ESColumn> cfgCols = mappings.getValue();
            Set<String> existCols = this.getMapping(indexName).stream().map((col) -> col.name).collect(Collectors.toSet());
            ;
            if (cfgCols.size() != existCols.size()) {
                throw new IllegalStateException("created schemaMapping cols size:"
                        + existCols.size()
                        + " is not equal with config schema mapping size:" + cfgCols.size());
            }

            for (int idx = 0; idx < cfgCols.size(); idx++) {
                cfg = cfgCols.get(idx);
                if (!existCols.contains(cfg.getName())) {
                    throw new IllegalStateException("column index:" + idx + ",config in TIS name:"
                            + cfg.getName() + " is not exist in elastic schema cols:" + String.join(",", existCols)
                            + ", please make a schema mapping synchronizing");
                }
            }
        }

        return false;
    }

    public JestResult execute(Action<JestResult> clientRequest) throws Exception {
        JestResult rst = null;
        rst = es.jestClient.execute(clientRequest);
        if (!rst.isSucceeded()) {
            //log.warn(rst.getErrorMessage());
        }
        return rst;
    }

    public Integer getStatus(JestResult rst) {
        JsonObject jsonObject = rst.getJsonObject();
        if (jsonObject.has("status")) {
            return jsonObject.get("status").getAsInt();
        }
        return 600;
    }

    public boolean isBulkResult(JestResult rst) {
        JsonObject jsonObject = rst.getJsonObject();
        return jsonObject.has("items");
    }


    public boolean alias(String indexname, String aliasname, boolean needClean) throws IOException {
        GetAliases getAliases = new GetAliases.Builder().addIndex(aliasname).build();
        AliasMapping addAliasMapping = new AddAliasMapping.Builder(indexname, aliasname).build();
        JestResult rst = es.jestClient.execute(getAliases);
        log.info(rst.getJsonString());
        List<AliasMapping> list = new ArrayList<AliasMapping>();
        if (rst.isSucceeded()) {
            JsonParser jp = new JsonParser();
            //            JSONObject jo = JSONObject.parseObject(rst.getJsonString());
            JsonObject jo = (JsonObject) jp.parse(rst.getJsonString());
            for (Map.Entry<String, JsonElement> entry : jo.entrySet()) {
                String tindex = entry.getKey();
                if (indexname.equals(tindex)) {
                    continue;
                }
                AliasMapping m = new RemoveAliasMapping.Builder(tindex, aliasname).build();
                String s = new Gson().toJson(m.getData());
                log.info(s);
                if (needClean) {
                    list.add(m);
                }
            }
        }

        ModifyAliases modifyAliases = new ModifyAliases.Builder(addAliasMapping).addAlias(list).setParameter(
                "master_timeout", "5m").build();
        rst = es.jestClient.execute(modifyAliases);
        if (!rst.isSucceeded()) {
            log.error(rst.getErrorMessage());
            return false;
        }
        return true;
    }

    public JestResult bulkInsert(Bulk.Builder bulk, int trySize) throws Exception {
        // es_rejected_execution_exception
        // illegal_argument_exception
        // cluster_block_exception
        JestResult rst = null;
        rst = es.jestClient.execute(bulk.build());
        if (!rst.isSucceeded()) {
            log.warn(rst.getErrorMessage());
        }
        return rst;
    }

    /**
     * 关闭JestClient客户端
     */
    public void closeJestClient() {
        if (es.jestClient != null) {
            es.jestClient.shutdownClient();
        }
    }

}
