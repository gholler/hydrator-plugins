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

import javax.ws.rs.Path;

import com.guavus.featureengineering.cdap.plugin.batch.aggregator.MultiInputGroupByCategoricalConfig.FunctionInfo;
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
@Name("MultiInputGroupByCategoricalAggregate")
@Description("Groups by one or more fields, then performs one or more aggregate functions on each group values. "
		+ "Supports valuecount, indicatorcount, nuniq as aggregate functions.")
@PluginInput(type = { "string:int:long string:int:long" })
@PluginOutput(type = { "list<int>" })
@PluginFunction(function = { "catcrossproduct" })
public class MultiInputGroupByCategoricalAggregator extends RecordAggregator {
	private final MultiInputGroupByCategoricalConfig conf;
	private List<String> groupByFields;
	private Map<String, List<String>> categoricalDictionaryMap;
	private List<MultiInputGroupByCategoricalConfig.FunctionInfo> functionInfos;
	private Map<String, MultiInputGroupByCategoricalConfig.FunctionInfo> functionInfoMap;
	private Schema outputSchema;
	private Map<String, AggregateFunction> aggregateFunctions;

	public MultiInputGroupByCategoricalAggregator(MultiInputGroupByCategoricalConfig conf) {
		super(conf.numPartitions);
		this.conf = conf;
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
		List<String> groupByFields = conf.getGroupByFields();
		List<MultiInputGroupByCategoricalConfig.FunctionInfo> aggregates = conf.getAggregates();

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
				getOutputSchema(inputSchema, groupByFields, aggregates, conf.getCategoricalDictionaryMap()));
	}

	@Override
	public void initialize(BatchRuntimeContext context) throws Exception {
		groupByFields = conf.getGroupByFields();
		functionInfos = conf.getAggregates();
		categoricalDictionaryMap = conf.getCategoricalDictionaryMap();
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
		for (Map.Entry<String, AggregateFunction> aggregateFunction : aggregateFunctions.entrySet()) {
			FunctionInfo functionInfo = functionInfoMap.get(aggregateFunction.getKey());
			List<String> dictionary = categoricalDictionaryMap.get(functionInfo.getName());

			// List<String> dictionary1 =
			// categoricalDictionaryMap.get(functionInfo.getField()[0].toLowerCase());
			// List<String> dictionary2 =
			// categoricalDictionaryMap.get(functionInfo.getField()[1].toLowerCase());

			Map<String, Map<String, Integer>> valueSpecificFrequencyCount = (Map<String, Map<String, Integer>>) aggregateFunction
					.getValue().getAggregate();
			for (String dict : dictionary) {
				String token[] = dict.trim().split("__");
				if (valueSpecificFrequencyCount.containsKey(token[1])) {
					Map<String, Integer> frequencyCount = valueSpecificFrequencyCount.get(token[1]);
					Integer count = frequencyCount.get(token[0]);
					if (count != null) {
						builder.set(aggregateFunction.getKey().toLowerCase() + "_" + dict.toLowerCase(), count);
					} else {
						builder.set(aggregateFunction.getKey().toLowerCase() + "_" + dict.toLowerCase(), 0);
					}
				} else {
					builder.set(aggregateFunction.getKey().toLowerCase() + "_" + dict.toLowerCase(), 0);
				}
			}

//			for (String dict2 : dictionary2) {
//				for (String dict1 : dictionary1) {
//					if (valueSpecificFrequencyCount.containsKey(dict2)) {
//						Map<String, Integer> frequencyCount = valueSpecificFrequencyCount.get(dict2);
//						Integer count = frequencyCount.get(dict1);
//						if (count != null) {
//							builder.set(aggregateFunction.getKey().toLowerCase() + "_" + dict1.toLowerCase() + "_"
//									+ dict2.toLowerCase(), count);
//						} else {
//							builder.set(aggregateFunction.getKey().toLowerCase() + "_" + dict1.toLowerCase() + "_"
//									+ dict2.toLowerCase(), 0);
//						}
//					} else {
//						builder.set(aggregateFunction.getKey().toLowerCase() + "_" + dict1.toLowerCase() + "_"
//								+ dict2.toLowerCase(), 0);
//					}
//				}
//			}
		}
		emitter.emit(builder.build());
	}

	@Path("outputSchema")
	public Schema getOutputSchema(GetSchemaRequest request) {
		return getOutputSchema(request.inputSchema, request.getGroupByFields(), request.getAggregates(),
				request.getCategoricalDictionaryMap());
	}

	private Schema getOutputSchema(Schema inputSchema, List<String> groupByFields,
			List<MultiInputGroupByCategoricalConfig.FunctionInfo> aggregates,
			Map<String, List<String>> categoricalDictionaryMap) {
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
		for (MultiInputGroupByCategoricalConfig.FunctionInfo functionInfo : aggregates) {
			String[] fields = functionInfo.getField();
			for (String field : functionInfo.getField()) {
				if (field == null) {
					throw new IllegalArgumentException(String.format(
							"Invalid aggregate %s(%s): Field '%s' does not exist in input schema %s.",
							functionInfo.getFunction(), functionInfo.getField(), functionInfo.getField(), inputSchema));
				}
				if (field.equalsIgnoreCase(functionInfo.getName())) {
					throw new IllegalArgumentException(String.format(
							"Name '%s' should not be same as aggregate field '%s'", functionInfo.getName(), field));
				}
			}

			Schema[] inputFieldSchema = new Schema[2];
			inputFieldSchema[0] = inputSchema.getField(fields[0]).getSchema();
			inputFieldSchema[1] = inputSchema.getField(fields[1]).getSchema();
			AggregateFunction aggregateFunction = functionInfo.getAggregateFunction(inputFieldSchema);

			List<String> dictionary = categoricalDictionaryMap.get(functionInfo.getName());
			if (dictionary != null)
				for (String dict2 : dictionary) {
					outputFields.add(Schema.Field.of(functionInfo.getName().toLowerCase() + "_" + dict2.toLowerCase(),
							aggregateFunction.getOutputSchema()));
				}
			else
				throw new IllegalStateException(
						"Input Categorical Dictionary isn't provided for input aggregate functions ="
								+ functionInfo.getName());

			// List<String> dictionary1 =
			// categoricalDictionaryMap.get(fields[0].toLowerCase());
			// List<String> dictionary2 =
			// categoricalDictionaryMap.get(fields[1].toLowerCase());
			// if (dictionary1 != null && dictionary2 != null)
			// for (String dict1 : dictionary1) {
			// for (String dict2 : dictionary2) {
			// outputFields
			// .add(Schema.Field.of(functionInfo.getName().toLowerCase() + "_" +
			// dict1.toLowerCase()
			// + "_" + dict2.toLowerCase(), aggregateFunction.getOutputSchema()));
			// }
			// }
			// else {
			// throw new IllegalStateException(
			// "Input Categorical Dictionary isn't provided for input aggregate functions =
			// "
			// + functionInfo.getName());
			// }
		}
		return Schema.recordOf(inputSchema.getRecordName() + ".aggMultiCat", outputFields);
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
		this.functionInfoMap = new HashMap<>();
		for (MultiInputGroupByCategoricalConfig.FunctionInfo functionInfo : functionInfos) {
			functionInfoMap.put(functionInfo.getName(), functionInfo);

			String[] fields = functionInfo.getField();
			Schema[] inputFieldSchema = new Schema[2];
			inputFieldSchema[0] = valueSchema.getField(fields[0]).getSchema();
			inputFieldSchema[1] = valueSchema.getField(fields[1]).getSchema();
			AggregateFunction aggregateFunction = functionInfo.getAggregateFunction(inputFieldSchema);
			List<String> dictionary = categoricalDictionaryMap.get(functionInfo.getName());
			// List<String> dictionary1 =
			// categoricalDictionaryMap.get(fields[0].toLowerCase());
			// List<String> dictionary2 =
			// categoricalDictionaryMap.get(fields[1].toLowerCase());

			aggregateFunction.beginFunction();

			for (String dict1 : dictionary) {
				outputFields.add(Schema.Field.of(functionInfo.getName().toLowerCase() + "_" + dict1.toLowerCase(),
						aggregateFunction.getOutputSchema()));
			}

			// if (dictionary2 != null && dictionary1 != null) {
			// for (String dict2 : dictionary2) {
			// for (String dict1 : dictionary1) {
			// outputFields
			// .add(Schema.Field.of(functionInfo.getName().toLowerCase() + "_" +
			// dict1.toLowerCase()
			// + "_" + dict2.toLowerCase(), aggregateFunction.getOutputSchema()));
			// }
			// }
			// }
			aggregateFunctions.put(functionInfo.getName(), aggregateFunction);
		}
		outputSchema = Schema.recordOf(valueSchema.getRecordName() + ".aggMultiCat", outputFields);
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
	public static class GetSchemaRequest extends MultiInputGroupByCategoricalConfig {
		private Schema inputSchema;
	}

}
