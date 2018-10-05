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

package com.guavus.featureengineering.cdap.plugin.batch.joiner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.Schema.Field;
import co.cask.cdap.etl.api.JoinConfig;
import co.cask.cdap.etl.api.JoinElement;
import co.cask.cdap.etl.api.MultiInputPipelineConfigurer;
import co.cask.cdap.etl.api.MultiInputStageConfigurer;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchJoinerContext;
import co.cask.cdap.etl.api.batch.BatchJoinerRuntimeContext;

/**
 * Batch joiner to join records from multiple inputs
 */
@Plugin(type = BatchJoiner.PLUGIN_TYPE)
@Name("JoinerDynamic")
@Description("Performs join operation on records from each input based on required inputs. If all the inputs are "
		+ "required inputs, inner join will be performed. Otherwise inner join will be performed on required inputs and "
		+ "records from non-required inputs will only be present if they match join criteria. If there are no required "
		+ "inputs, outer join will be performed")
public class JoinerDynamic extends BatchJoiner<StructuredRecord, StructuredRecord, StructuredRecord> {
	private final JoinerDynamicConfig conf;
	private Map<String, List<String>> perStageJoinKeys;
	private Table<String, String, String> sourceStageSelectedFields;
	private Set<String> requiredInputs;
	private Multimap<String, String> duplicateFields = ArrayListMultimap.create();
	private List<String> categoricalColumnsToBeCheckedList;
	Map<String, String> keysToBeAppendedMap;

	public JoinerDynamic(JoinerDynamicConfig conf) {
		this.conf = conf;
	}

	@Override
	public void configurePipeline(MultiInputPipelineConfigurer pipelineConfigurer) {
		MultiInputStageConfigurer stageConfigurer = pipelineConfigurer.getMultiInputStageConfigurer();
		init(null);
		// validate the input schema and get the output schema for it
		stageConfigurer.setOutputSchema(null);
	}

	@Override
	public void prepareRun(BatchJoinerContext context) throws Exception {
		if (conf.getNumPartitions() != null) {
			context.setNumPartitions(conf.getNumPartitions());
		}
	}

	@Override
	public void initialize(BatchJoinerRuntimeContext context) throws Exception {
		init(null);
	}

	@Override
	public StructuredRecord joinOn(String stageName, StructuredRecord record) throws Exception {
		List<Schema.Field> fields = new ArrayList<>();
		Schema schema = record.getSchema();

		List<String> joinKeys = perStageJoinKeys.get(stageName);
		int i = 1;
		for (String joinKey : joinKeys) {
			Schema.Field joinField = Schema.Field.of(String.valueOf(i++), schema.getField(joinKey).getSchema());
			fields.add(joinField);
		}
		Schema keySchema = Schema.recordOf("join.key", fields);
		StructuredRecord.Builder keyRecordBuilder = StructuredRecord.builder(keySchema);
		i = 1;
		for (String joinKey : joinKeys) {
			keyRecordBuilder.set(String.valueOf(i++), record.get(joinKey));
		}

		return keyRecordBuilder.build();
	}

	@Override
	public JoinConfig getJoinConfig() {
		return new JoinConfig(requiredInputs);
	}

	@Override
	public StructuredRecord merge(StructuredRecord joinKey, Iterable<JoinElement<StructuredRecord>> joinRow) {
		Map<String, Schema> inputSchema = getInputSchemaFromJoinRow(joinRow);
		StructuredRecord.Builder outRecordBuilder = StructuredRecord.builder(getOutputSchema(inputSchema));
		Set<String> schemaSet = new HashSet<String>();
		for (JoinElement<StructuredRecord> joinElement : joinRow) {
			String stageName = joinElement.getStageName();
			StructuredRecord record = joinElement.getInputRecord();
			Schema schema = inputSchema.get(stageName);
			Map<String, String> selectedFields = sourceStageSelectedFields.row(stageName);
			if (selectedFields != null && !selectedFields.isEmpty()) {
				for (Map.Entry<String, String> entry : selectedFields.entrySet()) {
					String selectedFieldName = entry.getKey();
					boolean added = false;
//					for (String categoricalColumn : categoricalColumnsToBeCheckedList) {
//						if (selectedFieldName.contains(categoricalColumn)) {
						if(schema.getField(selectedFieldName)==null) {
							List<Field> matchingFields = getAllMatchingSchemaFields(record.getSchema().getFields(),
									selectedFieldName);
							for (Field matchingField : matchingFields) {
								String outputFieldName = stageName + "_" + matchingField.getName() + "_";
								outRecordBuilder.set(outputFieldName, convertNANToZero(record.get(matchingField.getName())));
								added = true;
							}
						}
//						}
//					}
					if (added)
						continue;
					outRecordBuilder.set(entry.getValue(), convertNANToZero(record.get(selectedFieldName)));
				}
				continue;
			}
			Set<String> joinKeys = new HashSet<String>(this.perStageJoinKeys.get(stageName));
			String suffixToBeAdded = this.keysToBeAppendedMap.get(stageName);
			if (suffixToBeAdded == null)
				suffixToBeAdded = "";
			for (Schema.Field field : record.getSchema().getFields()) {
				String inputFieldName = field.getName();

				// drop the field if not part of selectedFields config input.
				if (selectedFields != null && !selectedFields.isEmpty()
						&& !selectedFields.containsKey(inputFieldName)) {
					continue;
				}
				String outputFieldName = "";
				// get output field from selectedFields config.
				if (selectedFields != null && !selectedFields.isEmpty()) {
					outputFieldName = selectedFields.get(inputFieldName);
				} else {
					outputFieldName = inputFieldName;
				}
				if (!joinKeys.contains(inputFieldName))
					outputFieldName += suffixToBeAdded;
				boolean result = schemaSet.add(outputFieldName);
				try {
					if (result)
						outRecordBuilder.set(outputFieldName, convertNANToZero(record.get(inputFieldName)));
				} catch (Throwable th) {
					// consuming this exception as this is expected behavior.
				}
			}
		}
		return outRecordBuilder.build();
	}

	private Object convertNANToZero(Object value) {
		try {
			if (Double.isNaN((double) value)) {
				return 0.0;
			}
		} catch (Throwable e) {
			try {
				if (Float.isNaN((float) value)) {
					return 0.0;
				}
			} catch (Throwable th) {
			}
		}
		return value;
	}

	private Map<String, Schema> getInputSchemaFromJoinRow(Iterable<JoinElement<StructuredRecord>> joinRow) {
		Map<String, Map<String, Field>> inputSchemaFields = new HashMap<>();
		for (JoinElement<StructuredRecord> joinElement : joinRow) {
			String stageName = joinElement.getStageName();
			Set<String> joinKeys = new HashSet<String>(this.perStageJoinKeys.get(stageName));
			StructuredRecord record = joinElement.getInputRecord();
			Map<String, Field> fieldMap = inputSchemaFields.get(stageName);
			if (fieldMap == null) {
				fieldMap = new HashMap<>();
				inputSchemaFields.put(stageName, fieldMap);
			}
			for (Field field : record.getSchema().getFields()) {
				if (fieldMap.containsKey(field.getName()))
					continue;
				fieldMap.put(field.getName(), Schema.Field.of(field.getName(), field.getSchema()));
			}
		}
		Map<String, Schema> inputSchema = new HashMap<String, Schema>();
		for (JoinElement<StructuredRecord> joinElement : joinRow) {
			String stageName = joinElement.getStageName();
			StructuredRecord record = joinElement.getInputRecord();
			if (inputSchema.containsKey(stageName))
				continue;
			inputSchema.put(stageName,
					Schema.recordOf(record.getSchema().getRecordName(), inputSchemaFields.get(stageName).values()));
		}
		return inputSchema;
	}

	private boolean contains(String name, List<String> categoricalColumnsToBeCheckedList2) {
		for (String s : categoricalColumnsToBeCheckedList2) {
			if (name.contains(s))
				return true;
		}
		return false;
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

	void init(Map<String, Schema> inputSchemas) {
		perStageJoinKeys = conf.getPerStageJoinKeys();
		requiredInputs = conf.getInputs();
		categoricalColumnsToBeCheckedList = conf.getCategoricalColumnsToBeChecked();
		sourceStageSelectedFields = conf.getPerStageSelectedFields();
		keysToBeAppendedMap = conf.getKeysToBeAppended();
	}

	Schema getOutputSchema(Map<String, Schema> inputSchemas) {
		validateRequiredInputs(inputSchemas);

		// stage name to input schema
		Map<String, Schema> inputs = new HashMap<>(inputSchemas);
		// Selected Field name to output field info
		Map<String, OutputFieldInfo> outputFieldInfo = new LinkedHashMap<>();
		List<String> duplicateAliases = new ArrayList<>();

		// order of fields in output schema will be same as order of selectedFields
		Set<Table.Cell<String, String, String>> rows = sourceStageSelectedFields.cellSet();

		Set<String> addedStages = new HashSet<String>();

		addColumsForSelectedFields(rows, outputFieldInfo, addedStages, inputs, duplicateAliases);
		Set<String> joinKeys = new HashSet<String>();
		addColumnsForRemainingInputStages(inputs, outputFieldInfo, duplicateAliases, joinKeys, addedStages);

		return Schema.recordOf("joinDyn.output", getOutputFields(outputFieldInfo, joinKeys));
	}

	private List<Field> getOutputFields(Map<String, OutputFieldInfo> outputFieldInfo, Set<String> joinKeys) {
		List<Schema.Field> outputFields = new ArrayList<>();
		for (String key : joinKeys) {
			outputFields.add(outputFieldInfo.get(key).getField());
			outputFieldInfo.remove(key);
		}
		for (OutputFieldInfo fieldInfo : outputFieldInfo.values()) {
			outputFields.add(fieldInfo.getField());
		}
		return outputFields;
	}

	private void addColumnsForRemainingInputStages(Map<String, Schema> inputs,
			Map<String, OutputFieldInfo> outputFieldInfo, List<String> duplicateAliases, Set<String> joinKeys2,
			Set<String> addedStages) {
		boolean addedJoinKeys = false;
		Map<String, String> stageSuffixMap = this.keysToBeAppendedMap;
		for (Map.Entry<String, Schema> entry : inputs.entrySet()) {
			String stageName = entry.getKey();
			if (addedStages.contains(stageName))
				continue;
			Schema inputSchema = entry.getValue();
			List<String> joinKeys = this.perStageJoinKeys.get(stageName);
			String suffixToBeAppended = stageSuffixMap.get(stageName);
			if (suffixToBeAppended == null)
				suffixToBeAppended = "";
			suffixToBeAppended = suffixToBeAppended.trim();
			Set<String> joinKeySet = new HashSet<String>();
			if (joinKeys != null) {
				joinKeySet.addAll(joinKeys);
			}
			Set<String> allFieldName = new HashSet<String>();
			for (Field inputField : inputSchema.getFields()) {
				String inputFieldName = inputField.getName();
				if (addedJoinKeys && joinKeySet.contains(inputFieldName)) {
					continue;
				}

				String alias = inputFieldName;
				if (!joinKeySet.contains(inputFieldName)) {
					alias += suffixToBeAppended;
				}
				allFieldName.add(inputFieldName);

				if (outputFieldInfo.containsKey(alias)) {
					OutputFieldInfo outInfo = outputFieldInfo.get(alias);
					if (duplicateAliases.add(alias)) {
						duplicateFields.put(outInfo.getStageName(), outInfo.getInputFieldName());
					}
					duplicateFields.put(stageName, inputFieldName);
					continue;
				}

				if (inputField == null) {
					throw new IllegalArgumentException(
							String.format("Invalid field: %s of stage '%s' does not exist in input schema %s.",
									inputFieldName, stageName, inputSchema));
				}
				// set nullable fields for non-required inputs
				if (requiredInputs.contains(stageName) || inputField.getSchema().isNullable()) {
					outputFieldInfo.put(alias, new OutputFieldInfo(alias, stageName, inputFieldName,
							Schema.Field.of(alias, inputField.getSchema())));
				} else {
					outputFieldInfo.put(alias, new OutputFieldInfo(alias, stageName, inputFieldName,
							Schema.Field.of(alias, Schema.nullableOf(inputField.getSchema()))));
				}
			}
			if (allFieldName.containsAll(joinKeySet)) {
				addedJoinKeys = true;
				joinKeys2.clear();
				joinKeys2.addAll(joinKeySet);
			}
		}

	}

	private void addColumsForSelectedFields(Set<Cell<String, String, String>> rows,
			Map<String, OutputFieldInfo> outputFieldInfo, Set<String> addedStages, Map<String, Schema> inputs,
			List<String> duplicateAliases) {
		for (Table.Cell<String, String, String> row : rows) {
			String stageName = row.getRowKey();
			String inputFieldName = row.getColumnKey();
			String alias = row.getValue();
			Schema inputSchema = inputs.get(stageName);
			if (inputSchema == null) {
				throw new IllegalArgumentException(
						String.format("Input schema for input stage %s can not be null", stageName));
			}
			addedStages.add(stageName);
			boolean added = false;
//			for (String categoricalColumn : categoricalColumnsToBeCheckedList) {
//				if (inputFieldName.contains(categoricalColumn)) {
				if(inputSchema.getField(inputFieldName)==null) {
					List<Field> matchingFields = getAllMatchingSchemaFields(inputSchema.getFields(), inputFieldName);
					for (Field matchingField : matchingFields) {
						String outputFieldName = stageName + "_" + matchingField.getName() + "_";

						// set nullable fields for non-required inputs
						if (requiredInputs.contains(stageName) || matchingField.getSchema().isNullable()) {
							outputFieldInfo.put(outputFieldName, new OutputFieldInfo(outputFieldName, stageName,
									outputFieldName, Schema.Field.of(outputFieldName, matchingField.getSchema())));
						} else {
							outputFieldInfo.put(outputFieldName, new OutputFieldInfo(outputFieldName, stageName,
									outputFieldName,
									Schema.Field.of(outputFieldName, Schema.nullableOf(matchingField.getSchema()))));
						}
						added = true;
					}
				}
//				}
//			}
			if (added)
				continue;

			if (outputFieldInfo.containsKey(alias)) {
				OutputFieldInfo outInfo = outputFieldInfo.get(alias);
				if (duplicateAliases.add(alias)) {
					duplicateFields.put(outInfo.getStageName(), outInfo.getInputFieldName());
				}
				duplicateFields.put(stageName, inputFieldName);
				continue;
			}

			Schema.Field inputField = inputSchema.getField(inputFieldName);
			if (inputField == null) {
				throw new IllegalArgumentException(
						String.format("Invalid field: %s of stage '%s' does not exist in input schema %s.",
								inputFieldName, stageName, inputSchema));
			}
			// set nullable fields for non-required inputs
			if (requiredInputs.contains(stageName) || inputField.getSchema().isNullable()) {
				outputFieldInfo.put(alias, new OutputFieldInfo(alias, stageName, inputFieldName,
						Schema.Field.of(alias, inputField.getSchema())));
			} else {
				outputFieldInfo.put(alias, new OutputFieldInfo(alias, stageName, inputFieldName,
						Schema.Field.of(alias, Schema.nullableOf(inputField.getSchema()))));
			}
		}
	}

	private List<Schema.Field> getOutputFields(Collection<OutputFieldInfo> fieldsInfo) {
		List<Schema.Field> outputFields = new ArrayList<>();
		for (OutputFieldInfo fieldInfo : fieldsInfo) {
			outputFields.add(fieldInfo.getField());
		}
		return outputFields;
	}

	/**
	 * Class to hold information about output fields
	 */
	private class OutputFieldInfo {
		private String name;
		private String stageName;
		private String inputFieldName;
		private Schema.Field field;

		OutputFieldInfo(String name, String stageName, String inputFieldName, Schema.Field field) {
			this.name = name;
			this.stageName = stageName;
			this.inputFieldName = inputFieldName;
			this.field = field;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getStageName() {
			return stageName;
		}

		public void setStageName(String stageName) {
			this.stageName = stageName;
		}

		public String getInputFieldName() {
			return inputFieldName;
		}

		public void setInputFieldName(String inputFieldName) {
			this.inputFieldName = inputFieldName;
		}

		public Schema.Field getField() {
			return field;
		}

		public void setField(Schema.Field field) {
			this.field = field;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			OutputFieldInfo that = (OutputFieldInfo) o;

			if (!name.equals(that.name)) {
				return false;
			}
			if (!stageName.equals(that.stageName)) {
				return false;
			}
			if (!inputFieldName.equals(that.inputFieldName)) {
				return false;
			}
			return field.equals(that.field);
		}

		@Override
		public int hashCode() {
			int result = name.hashCode();
			result = 31 * result + stageName.hashCode();
			result = 31 * result + inputFieldName.hashCode();
			result = 31 * result + field.hashCode();
			return result;
		}

		@Override
		public String toString() {
			return "OutputFieldInfo{" + "name='" + name + '\'' + ", stageName='" + stageName + '\''
					+ ", inputFieldName='" + inputFieldName + '\'' + ", field=" + field + '}';
		}
	}

	private void validateRequiredInputs(Map<String, Schema> inputSchemas) {
		for (String requiredInput : requiredInputs) {
			if (!inputSchemas.containsKey(requiredInput)) {
				throw new IllegalArgumentException(
						String.format("Provided required input %s is not an input stage name.", requiredInput));
			}
		}
	}
}
