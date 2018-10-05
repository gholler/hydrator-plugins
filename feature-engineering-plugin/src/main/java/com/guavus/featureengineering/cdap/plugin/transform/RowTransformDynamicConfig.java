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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.guavus.featureengineering.cdap.plugin.transform.function.Day;
import com.guavus.featureengineering.cdap.plugin.transform.function.Month;
import com.guavus.featureengineering.cdap.plugin.transform.function.NumCharacters;
import com.guavus.featureengineering.cdap.plugin.transform.function.NumWords;
import com.guavus.featureengineering.cdap.plugin.transform.function.PlusOneLog;
import com.guavus.featureengineering.cdap.plugin.transform.function.TransformFunction;
import com.guavus.featureengineering.cdap.plugin.transform.function.WeekDay;
import com.guavus.featureengineering.cdap.plugin.transform.function.Year;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;

/**
 * @author bhupesh.goel
 *
 */
public class RowTransformDynamicConfig extends PluginConfig {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4077946383526119822L;

	@Description("Transform function to compute on given records. "
			+ "Supported functions are day, year, month, weekday, numwords, characters. "
			+ "A function must specify the field it should be applied on, as well as the name it should be called. "
			+ "Transforms are specified using syntax: \"name:function(field)[, other functions]\"."
			+ "For example, 'dayOfWeek:day(timestamp),wordCount:numwords(description)' will calculate two transforms. "
			+ "The first will create a field called 'dayOfWeek' that is the day of the week of given timestamp. "
			+ "The second will create a field called 'wordCount' that contains the number of words in description column")
	private final String primitives;

	private final String categoricalColumnsToBeChecked;
	
	@VisibleForTesting
	RowTransformDynamicConfig(String primitives, String categoricalColumnsToBeChecked) {
		this.primitives = primitives;
		this.categoricalColumnsToBeChecked = categoricalColumnsToBeChecked;
	}

	/**
	 * 
	 */
	public RowTransformDynamicConfig() {
		this.primitives = "";
		this.categoricalColumnsToBeChecked = "";
	}

	List<String> getCategoricalColumnsToBeChecked() {
		List<String> categoricalColumnsToBeCheckedList = new LinkedList<String>();
		if(this.categoricalColumnsToBeChecked!=null || !this.categoricalColumnsToBeChecked.isEmpty()) {
			String tokens[] = categoricalColumnsToBeChecked.split(",");
			for(String token : tokens) {
				categoricalColumnsToBeCheckedList.add(token);
			}
		}
		return categoricalColumnsToBeCheckedList;
	}
	
	List<FunctionInfo> getPrimitives() {
		List<FunctionInfo> functionInfos = new ArrayList<>();
		Set<String> primitivesNames = new HashSet<>();
		for (String primitive : Splitter.on(',').trimResults().split(primitives)) {
			if(primitive.trim().isEmpty())
				continue;
			int colonIdx = primitive.indexOf(':');
			if (colonIdx < 0) {
				throw new IllegalArgumentException(String
						.format("Could not find ':' separating primitive name from its function in '%s'.", primitive));
			}
			String name = primitive.substring(0, colonIdx).trim();
			if (!primitivesNames.add(name)) {
				throw new IllegalArgumentException(
						String.format("Cannot create multiple primitive functions with the same name '%s'.", name));
			}

			String functionAndField = primitive.substring(colonIdx + 1).trim();
			int leftParanIdx = functionAndField.indexOf('(');
			if (leftParanIdx < 0) {
				throw new IllegalArgumentException(String.format(
						"Could not find '(' in function '%s'. Functions must be specified as function(field).",
						functionAndField));
			}
			String functionStr = functionAndField.substring(0, leftParanIdx).trim();
			Function function;
			try {
				function = Function.valueOf(functionStr.toUpperCase());
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException(String.format("Invalid function '%s'. Must be one of %s.",
						functionStr, Joiner.on(',').join(Function.values())));
			}

			if (!functionAndField.endsWith(")")) {
				throw new IllegalArgumentException(String.format(
						"Could not find closing ')' in function '%s'. Functions must be specified as function(field).",
						functionAndField));
			}
			String field = functionAndField.substring(leftParanIdx + 1, functionAndField.length() - 1).trim();
			if (field.isEmpty()) {
				throw new IllegalArgumentException(String
						.format("Invalid function '%s'. A field must be given as an argument.", functionAndField));
			}

			functionInfos.add(new FunctionInfo(name, field, function));
		}

//		if (functionInfos.isEmpty()) {
//			throw new IllegalArgumentException("The 'primitive' property must be set.");
//		}
		return functionInfos;
	}

	/**
	 * Class to hold information for an primitive function.
	 */
	static class FunctionInfo {
		private final String name;
		private final String field;
		private final Function function;

		FunctionInfo(String name, String field, Function function) {
			this.name = name;
			this.field = field;
			this.function = function;
		}

		public String getName() {
			return name;
		}

		public String getField() {
			return field;
		}

		public Function getFunction() {
			return function;
		}

		public TransformFunction getTransformFunction(Schema fieldSchema) {
			switch (function) {
			case DAY:
				return new Day(field, fieldSchema);
			case YEAR:
				return new Year(field, fieldSchema);
			case MONTH:
				return new Month(field, fieldSchema);
			case WEEKDAY:
				return new WeekDay(field, fieldSchema);
			case NUMWORDS:
				return new NumWords(field, fieldSchema);
			case NUMCHARACTERS:
				return new NumCharacters(field, fieldSchema);
			case PLUSONELOG:
				return new PlusOneLog(field, fieldSchema);
			}
			// should never happen
			throw new IllegalStateException("Unknown function type " + function);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			FunctionInfo that = (FunctionInfo) o;

			return Objects.equals(name, that.name) && Objects.equals(field, that.field)
					&& Objects.equals(function, that.function);
		}

		@Override
		public int hashCode() {
			return Objects.hash(name, field, function);
		}

		@Override
		public String toString() {
			return "FunctionInfo{" + "name='" + name + '\'' + ", field='" + field + '\'' + ", function=" + function
					+ '}';
		}
	}

	enum Function {
		DAY, YEAR, MONTH, WEEKDAY, NUMWORDS, NUMCHARACTERS, PLUSONELOG
	}
}
