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
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import com.guavus.featureengineering.cdap.plugin.transform.function.TimeDiffInMin;
import com.guavus.featureengineering.cdap.plugin.transform.function.TransformFunction;

/**
 * @author bhupesh.goel
 *
 */
public class MultiInputRowTransformConfig extends PluginConfig {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4077946383526119822L;

	@Description("Transform function to compute on given records. "
			+ "Supported functions are Time Difference. "
			+ "A function must specify all the field it should be applied on, as well as the name it should be called. "
			+ "Transforms are specified using syntax: \"name:function(field1 field2...)[, other functions]\"."
			+ "For example, 'timeDiff(timestamp1 timestamp2)' will calculate transforms. "
			+ "The first will create a field called 'timeDiff' that is the difference of given timestamps. ")
	private final String primitives;

	@VisibleForTesting
	MultiInputRowTransformConfig(String primitives) {
		this.primitives = primitives;
	}

	/**
	 * 
	 */
	public MultiInputRowTransformConfig() {
		this.primitives = "";
	}

	List<MultiFieldFunctionInfo> getPrimitives() {
		List<MultiFieldFunctionInfo> functionInfos = new ArrayList<>();
		Set<String> primitivesNames = new HashSet<>();
		for (String primitive : Splitter.on(',').trimResults().split(primitives)) {
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
			String[] fields = functionAndField.substring(leftParanIdx + 1, functionAndField.length() - 1).trim().split("\\s+");
			if (fields.length==0) {
				throw new IllegalArgumentException(String
						.format("Invalid function '%s'. A field must be given as an argument.", functionAndField));
			}

			functionInfos.add(new MultiFieldFunctionInfo(name, fields, function));
		}

		if (functionInfos.isEmpty()) {
			throw new IllegalArgumentException("The 'primitive' property must be set.");
		}
		return functionInfos;
	}

	/**
	 * Class to hold information for an primitive function.
	 */
	static class MultiFieldFunctionInfo {
		private final String name;
		private final String[] field;
		private final Function function;

		MultiFieldFunctionInfo(String name, String[] field, Function function) {
			this.name = name;
			this.field = field;
			this.function = function;
		}

		public String getName() {
			return name;
		}

		public String[] getField() {
			return field;
		}

		public Function getFunction() {
			return function;
		}

		public TransformFunction getTransformFunction(Schema[] fieldSchemas) {
			switch (function) {
			case TIMEDIFFINMIN:
				return new TimeDiffInMin(field, fieldSchemas);
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

			MultiFieldFunctionInfo that = (MultiFieldFunctionInfo) o;

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
		TIMEDIFFINMIN
	}
}
