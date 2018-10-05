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
@Name("MultiInputRowTransformDynamic")
@Description("Executes transform primitives to add new columns in record.")
@PluginInput(type = { "string string" })
@PluginOutput(type = { "int" })
@PluginFunction(function = { "timediffinmin" })
public class MultiInputRowTransformDynamic extends Transform<StructuredRecord, StructuredRecord> {

	private final MultiInputRowTransformConfig conf;
	private List<MultiInputRowTransformConfig.MultiFieldFunctionInfo> functionInfos;
	private Schema outputSchema;
	private Map<String, TransformFunction> transformFunctionsMap;

	public MultiInputRowTransformDynamic(MultiInputRowTransformConfig conf) {
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
	}

	private Schema getOutputSchema(Schema inputSchema, List<MultiInputRowTransformConfig.MultiFieldFunctionInfo> transformFunctions) {
		List<Schema.Field> outputFields = new ArrayList<>(inputSchema.getFields().size() + transformFunctions.size());
		outputFields.addAll(inputSchema.getFields());
		transformFunctionsMap = new LinkedHashMap<String, TransformFunction>();
		// check that all fields needed by aggregate functions exist in the input
		// schema.
		for (MultiInputRowTransformConfig.MultiFieldFunctionInfo functionInfo : transformFunctions) {
			Schema[] inputFieldSchema = new Schema[functionInfo.getField().length];
			int index=0;
			for(String functionField : functionInfo.getField()) {
				Schema.Field inputField = inputSchema.getField(functionField);
				if (inputField == null) {
					continue;
				}
				inputFieldSchema[index++] = inputField.getSchema();
			}
			
			TransformFunction transformFunction = functionInfo.getTransformFunction(inputFieldSchema);
			outputFields.add(Schema.Field.of(functionInfo.getName(), transformFunction.getOutputSchema()));
			transformFunctionsMap.put(functionInfo.getName(), transformFunction);
		}
		return Schema.recordOf(inputSchema.getRecordName() + ".multiTransDyn", outputFields);
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
		
		for(Map.Entry<String, TransformFunction> entry : transformFunctionsMap.entrySet()) {
			builder.set(entry.getKey(), entry.getValue().applyFunction(valueIn));
		}
		emitter.emit(builder.build());
	}
	
}
