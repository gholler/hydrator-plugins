/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.guavus.featureengineering.cdap.plugin.batch.aggregator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.guavus.featureengineering.cdap.plugin.batch.aggregator.MultiInputGroupByCategoricalConfigDynamicSchema.FunctionInfo;
import com.guavus.featureengineering.cdap.plugin.batch.aggregator.function.AggregateFunction;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.annotation.PluginFunction;
import co.cask.cdap.api.annotation.PluginInput;
import co.cask.cdap.api.annotation.PluginOutput;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;

/**
 * Batch group by aggregator.
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name("MultiInputGroupByCategoricalAggregateDynamic")
@Description("Groups by one or more fields, then performs one or more aggregate functions on each group values. "
		+ "Supports valuecount, indicatorcount, nuniq as aggregate functions.")
@PluginInput(type = { "string:int:long string:int:long" })
@PluginOutput(type = { "list<int>" })
@PluginFunction(function = { "catcrossproduct" })
public class MultiInputGroupByCategoricalAggregatorDynamicSchema extends RecordAggregator {
	private final MultiInputGroupByCategoricalConfigDynamicSchema conf;
	private List<String> groupByFields;
	private List<MultiInputGroupByCategoricalConfigDynamicSchema.FunctionInfo> functionInfos;
	private Map<String, MultiInputGroupByCategoricalConfigDynamicSchema.FunctionInfo> functionInfoMap;
	private Schema outputSchema;
	private Map<String, AggregateFunction> aggregateFunctions;

	public MultiInputGroupByCategoricalAggregatorDynamicSchema(MultiInputGroupByCategoricalConfigDynamicSchema conf) {
		super(conf.numPartitions);
		this.conf = conf;
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		stageConfigurer.setOutputSchema(null);
	}

	@Override
	public void initialize(BatchRuntimeContext context) throws Exception {
		groupByFields = conf.getGroupByFields();
		functionInfos = conf.getAggregates();
	}

	@Override
	public void groupBy(StructuredRecord record, Emitter<StructuredRecord> emitter) throws Exception {
		// app should provide some way to make some data calculated in configurePipeline
		// available here.
		// then we wouldn't have to calculate schema here
		StructuredRecord.Builder builder = StructuredRecord.builder(getGroupKeySchema(record.getSchema()));
		for (String groupByField : conf.getGroupByFields()) {
			builder.set(groupByField, record.get(groupByField));
		}
		emitter.emit(builder.build());
	}

	@Override
	public void aggregate(StructuredRecord groupKey, Iterator<StructuredRecord> iterator,
			Emitter<StructuredRecord> emitter) throws Exception {
		if (!iterator.hasNext()) {
			return;
		}

		StructuredRecord firstVal = iterator.next();
		initAggregates(firstVal.getSchema());

		updateAggregates(firstVal);

		while (iterator.hasNext()) {
			updateAggregates(iterator.next());
		}

		Schema outputSchema = getOutputSchema(firstVal.getSchema(), conf.getGroupByFields(), conf.getAggregates(),
				aggregateFunctions);
		StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
		for (String groupByField : groupByFields) {
			builder.set(groupByField, groupKey.get(groupByField));
		}

		for (Map.Entry<String, AggregateFunction> aggregateFunction : aggregateFunctions.entrySet()) {
			FunctionInfo functionInfo = functionInfoMap.get(aggregateFunction.getKey());

			Map<String, Map<String, Integer>> valueSpecificFrequencyCount = (Map<String, Map<String, Integer>>) aggregateFunction
					.getValue().getAggregate();

			for (String dict2 : valueSpecificFrequencyCount.keySet()) {
				Map<String, Integer> frequencyCount = valueSpecificFrequencyCount.get(dict2);
				for (String dict1 : frequencyCount.keySet()) {
					Integer count = frequencyCount.get(dict1);
					if (count != null) {
						builder.set(aggregateFunction.getKey().toLowerCase() + "_" + dict1.toLowerCase() + "__"
								+ dict2.toLowerCase(), count);
					} else {
						builder.set(aggregateFunction.getKey().toLowerCase() + "_" + dict1.toLowerCase() + "__"
								+ dict2.toLowerCase(), 0);
					}
				}
			}
		}
		emitter.emit(builder.build());
	}

	private Schema getOutputSchema(Schema inputSchema, List<String> groupByFields,
			List<MultiInputGroupByCategoricalConfigDynamicSchema.FunctionInfo> aggregates,
			Map<String, AggregateFunction> aggregateFunctions) {
		// Check that all the group by fields exist in the input schema,
		List<Schema.Field> outputFields = new ArrayList<>(groupByFields.size() + aggregates.size());
		for (String groupByField : groupByFields) {
			Schema.Field field = inputSchema.getField(groupByField);
			if (field == null) {
				continue;
			}
			outputFields.add(field);
		}

		// check that all fields needed by aggregate functions exist in the input
		// schema.
		for (MultiInputGroupByCategoricalConfigDynamicSchema.FunctionInfo functionInfo : aggregates) {
			String[] fields = functionInfo.getField();
			for (String field : functionInfo.getField()) {
				if (field == null) {
					continue;
				}
				if (field.equalsIgnoreCase(functionInfo.getName())) {
					throw new IllegalArgumentException(String.format(
							"Name '%s' should not be same as aggregate field '%s'", functionInfo.getName(), field));
				}
			}

			Schema[] inputFieldSchema = new Schema[2];
			inputFieldSchema[0] = inputSchema.getField(fields[0]).getSchema();
			inputFieldSchema[1] = inputSchema.getField(fields[1]).getSchema();
			AggregateFunction aggregateFunction = aggregateFunctions.get(functionInfo.getName());
			Map<String, Map<String, Integer>> valueSpecificFrequencyCount = (Map<String, Map<String, Integer>>) aggregateFunction
					.getAggregate();
			if (valueSpecificFrequencyCount != null) {
				for (String dict2 : valueSpecificFrequencyCount.keySet()) {
					Map<String, Integer> frequencyCount = valueSpecificFrequencyCount.get(dict2);
					for (String dict1 : frequencyCount.keySet()) {
						outputFields.add(Schema.Field.of(
								functionInfo.getName().toLowerCase() + "_" + dict1.toLowerCase() + "__"
										+ dict2.toLowerCase(),
								aggregateFunction.getOutputSchema().isNullable() ? aggregateFunction.getOutputSchema()
										: Schema.nullableOf(aggregateFunction.getOutputSchema())));
					}
				}
			} else {
				throw new IllegalStateException(
						"Input Categorical Dictionary isn't provided for input aggregate functions = "
								+ functionInfo.getName());
			}
		}
		return Schema.recordOf(inputSchema.getRecordName() + ".aggMultiCat", outputFields);
	}

	private void updateAggregates(StructuredRecord groupVal) {
		for (AggregateFunction aggregateFunction : aggregateFunctions.values()) {
			aggregateFunction.operateOn(groupVal);
		}
	}

	private void initAggregates(Schema valueSchema) {
		aggregateFunctions = new LinkedHashMap<>();
		this.functionInfoMap = new HashMap<>();
		for (MultiInputGroupByCategoricalConfigDynamicSchema.FunctionInfo functionInfo : functionInfos) {
			functionInfoMap.put(functionInfo.getName(), functionInfo);

			String[] fields = functionInfo.getField();
			Schema[] inputFieldSchema = new Schema[2];
			inputFieldSchema[0] = valueSchema.getField(fields[0]).getSchema();
			inputFieldSchema[1] = valueSchema.getField(fields[1]).getSchema();
			AggregateFunction aggregateFunction = functionInfo.getAggregateFunction(inputFieldSchema);

			aggregateFunction.beginFunction();

			aggregateFunctions.put(functionInfo.getName(), aggregateFunction);
		}
	}

	private Schema getGroupKeySchema(Schema inputSchema) {
		List<Schema.Field> fields = new ArrayList<>();
		for (String groupByField : conf.getGroupByFields()) {
			Schema.Field fieldSchema = inputSchema.getField(groupByField);
			if (fieldSchema == null) {
				throw new IllegalArgumentException(
						String.format("Cannot group by field '%s' because it does not exist in input schema %s",
								groupByField, inputSchema));
			}
			fields.add(fieldSchema);
		}
		return Schema.recordOf("group.key.schema", fields);
	}

}
