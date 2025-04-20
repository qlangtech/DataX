package com.alibaba.datax.core.job;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.transformer.TransformerExecution;
import com.alibaba.datax.core.transport.transformer.TransformerExecutionParas;
import com.alibaba.datax.core.transport.transformer.TransformerInfo;
import com.alibaba.datax.core.util.TISComplexTransformer;
import com.alibaba.datax.core.util.TransformerBuildInfo;
import com.alibaba.datax.core.util.container.TransformerConstant;
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.plugin.datax.transformer.OutputParameter;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformer;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugin.ds.RunningContext;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * no comments.
 * Created by liqiang on 16/3/9.
 */
public class TransformerUtil {

    private static final Logger LOG = LoggerFactory.getLogger(TransformerUtil.class);


    static TransformerBuildInfo buildTransformerInfo(IJobContainerContext containerContext, Configuration taskConfig) {

        final String tabRelevantTransformer = taskConfig.getString(TransformerConstant.JOB_TRANSFORMER_NAME);
        // Transformer 生成的出参
        final List<String> relevantKeys = taskConfig.getList(TransformerConstant.JOB_TRANSFORMER_RELEVANT_KEYS, String.class);
        if (StringUtils.isEmpty(tabRelevantTransformer)) {
            throw new IllegalArgumentException("tabRelevantTransformer name can not be null");
        }
        List<TransformerExecution> result = Lists.newArrayList();
        TransformerInfo transformerInfo = null;
        TransformerExecution texec = null;
        DataXName dataXName = containerContext.getCollectionName();
        IPluginContext pluginContext = IPluginContext.namedContext(containerContext.getCollectionName().getPipelineName());
        Optional<RecordTransformerRules> transformersOpt = RecordTransformerRules.loadTransformerRules(
                pluginContext, dataXName.getType(), dataXName.getPipelineName(), tabRelevantTransformer);
        RecordTransformerRules transformers = null;
        if (transformersOpt == null
                || (transformers = transformersOpt.orElseThrow(() -> new IllegalStateException("tabRelevantTransformer:"
                + tabRelevantTransformer + " relevant transformersOpt must be present"))) == null
                || CollectionUtils.isEmpty(transformers.rules)) {
            throw new IllegalStateException("transformer:" + tabRelevantTransformer + " can not be empty");
        }

        for (RecordTransformer t : transformers.rules) {
            transformerInfo = new TransformerInfo();
            transformerInfo.setTransformer(new TISComplexTransformer(t.getUdf()));
            texec = new TransformerExecution(transformerInfo, new TransformerExecutionParas());
            texec.setIsChecked(true);
            result.add(texec);
        }
        List<String> fromPlugnKeys = transformers.relevantColKeys();
        if (!CollectionUtils.isEqualCollection(relevantKeys, fromPlugnKeys)) {
            throw TisException.create("pipeline:" + pluginContext.getCollectionName()
                    + "table:" + tabRelevantTransformer + ",relevant keys from dataX config:" + String.join(",", relevantKeys)
                    + "\n is not equal with key build from plugin:" + String.join(",", fromPlugnKeys)
                    + "\n Please regenerate the DataX Config Files then reTrigger pipeline again!!!");
        }

        final ITransformerBuildInfo transformerCfg = transformers.createTransformerBuildInfo(pluginContext);

        return new TransformerBuildInfo() {
            @Override
            public List<TransformerExecution> getExecutions() {
                return result;
            }

            @Override
            public <CONTEXT extends RunningContext> Map<String, Function<CONTEXT, Object>> contextParamValsGetter() {
                return transformerCfg.contextParamValsGetter();
            }

            @Override
            public List<IColMetaGetter> originColsWithContextParams() {
                return transformerCfg.originColsWithContextParams();
            }

            @Override
            public List<OutputParameter> tranformerColsWithoutContextParams() {
                return transformerCfg.tranformerColsWithoutContextParams();
            }

            @Override
            public boolean containContextParams() {
                return transformerCfg.containContextParams();
            }

            @Override
            public Map<String, Object> contextParamVals(RunningContext runningContext) {
                return transformerCfg.contextParamVals(runningContext);
            }

            @Override
            public <T extends IColMetaGetter> List<OutputParameter> overwriteColsWithContextParams(List<T> sourceCols) {
                return transformerCfg.overwriteColsWithContextParams(sourceCols);
            }
        };
    }
}
