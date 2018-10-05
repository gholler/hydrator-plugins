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
package com.guavus.featureengineering.cdap.plugin.transform;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.guavus.featureengineering.cdap.plugin.transform.function.TransformFunction;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.annotation.PluginFunction;
import co.cask.cdap.api.annotation.PluginInput;
import co.cask.cdap.api.annotation.PluginOutput;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.Schema.Field;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;

/**
 * @author bhupesh.goel
 *
 */
@Plugin(type = "transform")
@Name("RowTransformDynamic")
@Description("Executes transform primitives to add new columns in record.")
@PluginInput(type = { "string", "string", "string", "string", "string", "string", "double:int:long:float" })
@PluginOutput(type = { "int", "int", "int", "int", "int", "int", "double" })
@PluginFunction(function = { "day", "year", "month", "weekday", "numwords", "numcharacters", "plusonelog" })
public class RowTransformDynamic extends Transform<StructuredRecord, StructuredRecord> {

	private final RowTransformDynamicConfig conf;
	private List<RowTransformDynamicConfig.FunctionInfo> functionInfos;
	private List<String> categoricalColumnsToBeCheckedList;
	private Map<String, TransformFunction> transformFunctionsMap;

	public RowTransformDynamic(RowTransformDynamicConfig conf) {
		this.conf = conf;
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		stageConfigurer.setOutputSchema(null);
	}

	// initialize is called once at the start of each pipeline run
	@Override
	public void initialize(TransformContext context) throws Exception {
		super.initialize(context);
		functionInfos = conf.getPrimitives();
		this.categoricalColumnsToBeCheckedList = conf.getCategoricalColumnsToBeChecked();
	}

	private Schema getOutputSchema(Schema inputSchema,
			List<RowTransformDynamicConfig.FunctionInfo> transformFunctions) {
		List<Schema.Field> outputFields = new ArrayList<>();
		outputFields.addAll(inputSchema.getFields());
		transformFunctionsMap = new LinkedHashMap<String, TransformFunction>();
		// check that all fields needed by aggregate functions exist in the input
		// schema.
		for (RowTransformDynamicConfig.FunctionInfo functionInfo : transformFunctions) {
			Schema.Field inputField = inputSchema.getField(functionInfo.getField());
			if (inputField == null) {
				// could be the case that column is categorical column and exists in extended
				// form along with dictionary in schema.
//				for (String categoricalColumn : categoricalColumnsToBeCheckedList) {
//					if (functionInfo.getField().contains(categoricalColumn)) {
						List<Field> matchingFields = getAllMatchingSchemaFields(inputSchema.getFields(),
								functionInfo.getField());
						for (Field matchingField : matchingFields) {
							String outputFieldName = functionInfo.getFunction().name().toLowerCase() + "_"
									+ matchingField.getName() + "_";
							TransformFunction transformFunction = functionInfo
									.getTransformFunction(matchingField.getSchema());
							outputFields.add(Schema.Field.of(outputFieldName, transformFunction.getOutputSchema()));
							transformFunctionsMap.put(outputFieldName, transformFunction);
						}
//					}
//				}
				continue;
			}

			TransformFunction transformFunction = functionInfo.getTransformFunction(inputField.getSchema());

			outputFields.add(Schema.Field.of(functionInfo.getName(), transformFunction.getOutputSchema()));
			transformFunctionsMap.put(functionInfo.getName(), transformFunction);
		}
		return Schema.recordOf(inputSchema.getRecordName() + ".trans", outputFields);
	}

	private List<Field> getAllMatchingSchemaFields(List<Field> fields, String fieldName) {
		List<Field> matchingFields = new LinkedList<Field>();
		for (Field field : fields) {
			if (field.getName().contains(fieldName)) {
				matchingFields.add(field);
			}
		}
		return matchingFields;
	}

	@Override
	public void transform(StructuredRecord valueIn, Emitter<StructuredRecord> emitter) throws Exception {
		Schema inputSchema = valueIn.getSchema();
		Schema outputSchema = getOutputSchema(inputSchema, functionInfos);
		StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);

		for (Schema.Field inputField : inputSchema.getFields()) {
			String inputFieldName = inputField.getName();
			Object inputVal = valueIn.get(inputFieldName);
			builder.set(inputFieldName, inputVal);
		}

		for (Map.Entry<String, TransformFunction> entry : transformFunctionsMap.entrySet()) {
			builder.set(entry.getKey(), entry.getValue().applyFunction(valueIn));
		}
		emitter.emit(builder.build());
	}

}
