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

import javax.ws.rs.Path;

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
import com.guavus.featureengineering.cdap.plugin.batch.aggregator.function.AggregateFunction;
/**
 * Batch group by aggregator.
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name("GroupByCategoricalAggregate")
@Description("Groups by one or more fields, then performs one or more aggregate functions on each group values. "
		+ "Supports valuecount, indicatorcount, nuniq as aggregate functions.")
@PluginInput(type = { "string:int:long", "string:int:long" })
@PluginOutput(type = { "list<int>", "list<int>" })
@PluginFunction(function = { "valuecount", "indicatorcount" })
public class GroupByCategoricalAggregator extends RecordAggregator {
	private final GroupByCategoricalConfig conf;
	private List<String> groupByFields;
	private List<String> categoricalDictionary;
	private List<GroupByCategoricalConfig.FunctionInfo> functionInfos;
	private Schema outputSchema;
	private Map<String, AggregateFunction> aggregateFunctions;

	public GroupByCategoricalAggregator(GroupByCategoricalConfig conf) {
		super(conf.numPartitions);
		this.conf = conf;
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
		List<String> groupByFields = conf.getGroupByFields();
		List<GroupByCategoricalConfig.FunctionInfo> aggregates = conf.getAggregates();

		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		Schema inputSchema = stageConfigurer.getInputSchema();
		// if null, the input schema is unknown, or its multiple schemas.
		if (inputSchema == null) {
			stageConfigurer.setOutputSchema(null);
			return;
		}

		// otherwise, we have a constant input schema. Get the output schema and
		// propagate the schema, which is group by fields + aggregate fields
		stageConfigurer.setOutputSchema(
				getOutputSchema(inputSchema, groupByFields, aggregates, conf.getCategoricalDictionary()));
	}

	@Override
	public void initialize(BatchRuntimeContext context) throws Exception {
		groupByFields = conf.getGroupByFields();
		functionInfos = conf.getAggregates();
		categoricalDictionary = conf.getCategoricalDictionary();
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
		StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
		for (String groupByField : groupByFields) {
			builder.set(groupByField, groupKey.get(groupByField));
		}
		updateAggregates(firstVal);

		while (iterator.hasNext()) {
			updateAggregates(iterator.next());
		}
		int i = 0;
		for (Map.Entry<String, AggregateFunction> aggregateFunction : aggregateFunctions.entrySet()) {
			String dictionary = categoricalDictionary.get(i++);
			Map<String, Integer> frequencyCount = (Map<String, Integer>) aggregateFunction.getValue().getAggregate();
			for (String token : dictionary.split(";")) {
				if (frequencyCount.containsKey(token.toLowerCase()))
					builder.set(aggregateFunction.getKey() + "_" + token.toLowerCase(), frequencyCount.get(token));
				else
					builder.set(aggregateFunction.getKey() + "_" + token.toLowerCase(), 0);
			}
		}
		emitter.emit(builder.build());
	}

	@Path("outputSchema")
	public Schema getOutputSchema(GetSchemaRequest request) {
		return getOutputSchema(request.inputSchema, request.getGroupByFields(), request.getAggregates(),
				request.getCategoricalDictionary());
	}

	private Schema getOutputSchema(Schema inputSchema, List<String> groupByFields,
			List<GroupByCategoricalConfig.FunctionInfo> aggregates, List<String> categoricalDictionary) {
		// Check that all the group by fields exist in the input schema,
		List<Schema.Field> outputFields = new ArrayList<>(groupByFields.size() + aggregates.size());
		for (String groupByField : groupByFields) {
			Schema.Field field = inputSchema.getField(groupByField);
			if (field == null) {
				throw new IllegalArgumentException(
						String.format("Cannot group by field '%s' because it does not exist in input schema %s.",
								groupByField, inputSchema));
			}
			outputFields.add(field);
		}

		// check that all fields needed by aggregate functions exist in the input
		// schema.
		int index = 0;
		for (GroupByCategoricalConfig.FunctionInfo functionInfo : aggregates) {
			String dictionary = categoricalDictionary.get(index++);
			Schema.Field inputField = inputSchema.getField(functionInfo.getField());
			if (inputField == null) {
				throw new IllegalArgumentException(String.format(
						"Invalid aggregate %s(%s): Field '%s' does not exist in input schema %s.",
						functionInfo.getFunction(), functionInfo.getField(), functionInfo.getField(), inputSchema));
			}
			if (functionInfo.getField().equalsIgnoreCase(functionInfo.getName())) {
				throw new IllegalArgumentException(String.format("Name '%s' should not be same as aggregate field '%s'",
						functionInfo.getName(), functionInfo.getField()));
			}
			AggregateFunction aggregateFunction = functionInfo.getAggregateFunction(inputField.getSchema());
			if (dictionary != null) {
				String tokens[] = dictionary.split(";");
				for (String token : tokens) {
					outputFields.add(Schema.Field.of(functionInfo.getName() + "_" + token.toLowerCase(),
							aggregateFunction.getOutputSchema()));
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
		List<Schema.Field> outputFields = new ArrayList<>(groupByFields.size() + functionInfos.size());
		for (String groupByField : groupByFields) {
			outputFields.add(valueSchema.getField(groupByField));
		}

		aggregateFunctions = new LinkedHashMap<>();
		int index = 0;
		for (GroupByCategoricalConfig.FunctionInfo functionInfo : functionInfos) {
			String dictionary = categoricalDictionary.get(index++);
			Schema.Field inputField = valueSchema.getField(functionInfo.getField());
			Schema fieldSchema = inputField == null ? null : inputField.getSchema();
			AggregateFunction aggregateFunction = functionInfo.getAggregateFunction(fieldSchema);
			aggregateFunction.beginFunction();

			if (dictionary != null) {
				String tokens[] = dictionary.split(";");
				for (String token : tokens) {
					outputFields.add(Schema.Field.of(functionInfo.getName() + "_" + token.toLowerCase(),
							aggregateFunction.getOutputSchema()));
				}
			}
			aggregateFunctions.put(functionInfo.getName(), aggregateFunction);
		}
		outputSchema = Schema.recordOf(valueSchema.getRecordName() + ".aggCat", outputFields);
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

	/**
	 * Endpoint request for output schema.
	 */
	public static class GetSchemaRequest extends GroupByCategoricalConfig {
		private Schema inputSchema;
	}

}
