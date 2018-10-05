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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
@Name("GroupByCategoricalAggregateDynamic")
@Description("Groups by one or more fields, then performs one or more aggregate functions on each group values. "
		+ "Supports valuecount, indicatorcount, nuniq as aggregate functions.")
@PluginInput(type = { "string:int:long", "string:int:long" })
@PluginOutput(type = { "list<int>", "list<int>" })
@PluginFunction(function = { "valuecount", "indicatorcount" })
public class GroupByCategoricalAggregatorDynamicSchema extends RecordAggregator {
	private final GroupByCategoricalConfigDynamicSchema conf;
	private List<String> groupByFields;
	private List<GroupByCategoricalConfigDynamicSchema.FunctionInfo> functionInfos;
	private Schema outputSchema;
	private Map<String, AggregateFunction> aggregateFunctions;

	public GroupByCategoricalAggregatorDynamicSchema(GroupByCategoricalConfigDynamicSchema conf) {
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
		Schema outputSchema = generateOutputSchema(firstVal.getSchema(), conf.getGroupByFields(), conf.getAggregates(), aggregateFunctions);
		StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
		for (String groupByField : groupByFields) {
			builder.set(groupByField, groupKey.get(groupByField));
		}
		
		for (Map.Entry<String, AggregateFunction> aggregateFunction : aggregateFunctions.entrySet()) {
			Map<String, Integer> frequencyCount = (Map<String, Integer>) aggregateFunction.getValue().getAggregate();
			for (String token : frequencyCount.keySet()) {
				builder.set(aggregateFunction.getKey() + "_" + token.toLowerCase(), frequencyCount.get(token));
			}
		}
		emitter.emit(builder.build());
	}

	private Schema generateOutputSchema(Schema inputSchema, List<String> groupByFields,
			List<GroupByCategoricalConfigDynamicSchema.FunctionInfo> aggregates, Map<String, AggregateFunction> aggregateFunctions2) {
		// Check that all the group by fields exist in the input schema,
		List<Schema.Field> outputFields = new ArrayList<>();
		for (String groupByField : groupByFields) {
			Schema.Field field = inputSchema.getField(groupByField);
			if (field == null) {
				continue;
			}
			outputFields.add(field);
		}

		// check that all fields needed by aggregate functions exist in the input
		// schema.
		for (GroupByCategoricalConfigDynamicSchema.FunctionInfo functionInfo : aggregates) {
			Schema.Field inputField = inputSchema.getField(functionInfo.getField());
			if (inputField == null) {
				continue;
			}
			if (functionInfo.getField().equalsIgnoreCase(functionInfo.getName())) {
				throw new IllegalArgumentException(String.format("Name '%s' should not be same as aggregate field '%s'",
						functionInfo.getName(), functionInfo.getField()));
			}
			AggregateFunction aggregateFunction = aggregateFunctions2.get(functionInfo.getName());
			Map<String, Integer> frequencyCount = (Map<String, Integer>) aggregateFunction.getAggregate();
			if (frequencyCount != null) {
				for (String token : frequencyCount.keySet()) {
					outputFields.add(Schema.Field.of(functionInfo.getName() + "_" + token.toLowerCase(),
							aggregateFunction.getOutputSchema().isNullable()?aggregateFunction.getOutputSchema():Schema.nullableOf(aggregateFunction.getOutputSchema())));
				}
			} else {
				throw new IllegalStateException(
						"Input Categorical Dictionary isn't provided for input aggregate functions = "
								+ functionInfo.getName());
			}
		}
		return Schema.recordOf(inputSchema.getRecordName() + ".aggCat", outputFields);
	}

	private void updateAggregates(StructuredRecord groupVal) {
		for (AggregateFunction aggregateFunction : aggregateFunctions.values()) {
			aggregateFunction.operateOn(groupVal);
		}
	}

	private void initAggregates(Schema valueSchema) {
		aggregateFunctions = new LinkedHashMap<>();
		for (GroupByCategoricalConfigDynamicSchema.FunctionInfo functionInfo : functionInfos) {
			Schema.Field inputField = valueSchema.getField(functionInfo.getField());
			Schema fieldSchema = inputField == null ? null : inputField.getSchema();
			if(fieldSchema==null)
				continue;
			AggregateFunction aggregateFunction = functionInfo.getAggregateFunction(fieldSchema);
			aggregateFunction.beginFunction();

			aggregateFunctions.put(functionInfo.getName(), aggregateFunction);
		}
	}

	private Schema getGroupKeySchema(Schema inputSchema) {
		List<Schema.Field> fields = new ArrayList<>();
		for (String groupByField : conf.getGroupByFields()) {
			Schema.Field fieldSchema = inputSchema.getField(groupByField);
			if (fieldSchema == null) {
				continue;
			}
			fields.add(fieldSchema);
		}
		return Schema.recordOf("group.key.schema", fields);
	}

}
