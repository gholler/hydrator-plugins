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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.Path;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.format.StructuredRecord.Builder;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.Schema.Field;
import co.cask.cdap.api.data.schema.Schema.Type;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.common.enums.FeatureSTATS;
import scala.Tuple2;

/*TODO:This implementation is unoptimized and doesn't take advantage of spark for computing string stats and percentiles. 
 * Need to modify this implementation via adding similar logic of taking matrix transposes in spark.
 * Have a look at https://recalll.co/ask/v/topic/scala-How-to-transpose-an-RDD-in-Spark/557a43a52bd273720d8b892f
 * */

/**
 * SparkCompute plugin that generates different stats for given schema.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(StatsComputeInMemory.NAME)
@Description("Computes statistics for each schema column.")
public class StatsComputeInMemory extends SparkCompute<StructuredRecord, StructuredRecord> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3718869622401824253L;
	/**
	 * 
	 */
	public static final String NAME = "StatsComputeInMemory";
	private final Conf config;
	private int numberOfThreads;
	private static final double percentiles[] = { 0.25, 0.5, 0.75 };

	/**
	 * Config properties for the plugin.
	 */
	@VisibleForTesting
	public static class Conf extends PluginConfig {

		@Description("The field from the input records containing the words to count.")
		private String parallelThreads;

		Conf(String parallelThreads) {
			this.parallelThreads = parallelThreads;
		}

		Conf() {
			this.parallelThreads = "";
		}

		int getParallelThreads() {
			Iterable<String> parallelTh = Splitter.on(',').trimResults().split(parallelThreads);
			try {
				return Integer.parseInt(parallelTh.iterator().next());
			} catch (Exception e) {
				return 10;
			}
		}
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		this.numberOfThreads = config.getParallelThreads();
	}

	/**
	 * Endpoint request for output schema.
	 */
	public static class GetSchemaRequest extends Conf {
		private Schema inputSchema;
	}

	private static class Identity<T> implements Function<T, T> {
		@Override
		public T call(T t) throws Exception {
			return t;
		}
	}

	private static class CountFunction implements PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, Long> {

		@Override
		public Iterable<Tuple2<String, Long>> call(Tuple2<String, Iterable<String>> tuples) throws Exception {
			String word = tuples._1();
			Long count = 0L;
			for (String s : tuples._2()) {
				count++;
			}
			List<Tuple2<String, Long>> output = new ArrayList<>();
			output.add(new Tuple2<>(word, count));
			return output;
		}
	}

	public StatsComputeInMemory(Conf config) {
		this.config = config;
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
		pipelineConfigurer.getStageConfigurer().setOutputSchema(null);
	}

	@Path("outputSchema")
	public Schema getOutputSchema(GetSchemaRequest request) {
		return getOutputSchema(request.inputSchema);
	}

	private Schema getOutputSchema(Schema inputSchema) {
		List<Schema.Field> outputFields = new ArrayList<>();
		outputFields.add(Schema.Field.of("Statistic", Schema.of(Schema.Type.STRING)));
		for (Schema.Field field : inputSchema.getFields()) {

			outputFields.add(Schema.Field.of(field.getName(), Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))));

		}
		return Schema.recordOf(inputSchema.getRecordName() + ".statsDyn", outputFields);
	}

	private Schema getVerticalFeaturesOutputSchema(Schema inputSchema) {
		List<Schema.Field> outputFields = new ArrayList<>();
		outputFields.add(Schema.Field.of("Id", Schema.of(Schema.Type.LONG)));
		for (FeatureSTATS stats : FeatureSTATS.values()) {
			outputFields.add(
					Schema.Field.of(stats.getName(), Schema.nullableOf(Schema.of(getSchemaType(stats.getType())))));
		}
		return Schema.recordOf(inputSchema.getRecordName() + ".stats", outputFields);
	}

	private Type getSchemaType(final String type) {
		switch (type) {
		case "double":
			return Schema.Type.DOUBLE;
		case "long":
			return Schema.Type.LONG;
		case "string":
			return Schema.Type.STRING;
		}
		return null;
	}

	private List<List<Double>> getPercentileStatsRDD(List<List<Double>> doubleListRDD,
			SparkExecutionPluginContext sparkExecutionPluginContext, int length, long size)
			throws InterruptedException, ExecutionException {
		List<List<Double>> percentileValues = new LinkedList<List<Double>>();
		for (int i = 0; i < percentiles.length; i++) {
			percentileValues.add(new LinkedList<Double>());
		}

		for (int i = 0; i < doubleListRDD.get(0).size(); i++) {
			List<Double> valList = new ArrayList<Double>();
			for (int j = 0; j < doubleListRDD.size(); j++) {
				valList.add(doubleListRDD.get(j).get(i));
			}
			Collections.sort(valList);
			for (int j = 0; j < percentiles.length; j++) {
				double percentile = percentiles[j];
				int id = (int) (size * percentile);
				percentileValues.get(j).add(valList.get(id));
			}
		}
		return percentileValues;
	}

	private JavaRDD<List<String>> getStringListRDD(JavaRDD<StructuredRecord> javaRDD, final List<Field> inputField) {

		return javaRDD.map(new Function<StructuredRecord, List<String>>() {

			@Override
			public List<String> call(StructuredRecord record) throws Exception {
				List<String> values = new LinkedList<String>();
				for (Schema.Field field : inputField) {
					if (!getSchemaType(field.getSchema()).equals(Schema.Type.STRING))
						continue;
					Object val = record.get(field.getName());
					values.add(String.valueOf(val));
				}
				return values;
			}
		});

	}

	@Override
	public JavaRDD<StructuredRecord> transform(final SparkExecutionPluginContext sparkExecutionPluginContext,
			JavaRDD<StructuredRecord> javaRDD) throws Exception {
		// final List<Schema.Field> inputField =
		// sparkExecutionPluginContext.getInputSchema().getFields();
		// Schema outputSchema = sparkExecutionPluginContext.getOutputSchema();
		long size = javaRDD.count();
		if (size == 0)
			return sparkExecutionPluginContext.getSparkContext().parallelize(new LinkedList<StructuredRecord>());
		try {
			javaRDD.first();
		} catch (Throwable th) {
			return sparkExecutionPluginContext.getSparkContext().parallelize(new LinkedList<StructuredRecord>());
		}
		Schema inputSchema = getInputSchema(javaRDD);
		final List<Schema.Field> inputField = inputSchema.getFields();
		Schema outputSchema = getOutputSchema(inputSchema);

		JavaRDD<Vector> vectoredRDD = getVectorRDD(javaRDD, inputField);
		MultivariateStatisticalSummary summary = Statistics.colStats(vectoredRDD.rdd());

		JavaRDD<List<String>> stringListRDD = getStringListRDD(javaRDD, inputField);
		List<String> maxOccuringStringEntryList = new LinkedList<>();
		List<List<Long>> stringStats = getStringStats(stringListRDD.collect(), inputField, maxOccuringStringEntryList);
		stringListRDD.unpersist();

		JavaRDD<List<Double>> doubleListRDD = getDoubleListRDD(javaRDD, inputField);
		List<List<Double>> percentileScores = getPercentileStatsRDD(doubleListRDD.collect(),
				sparkExecutionPluginContext, summary.variance().toArray().length, size);
		doubleListRDD.unpersist();
		// List<StructuredRecord> recordList = createStructuredRecord(summary,
		// stringStats, percentileScores, outputSchema, inputSchema);
		List<StructuredRecord> recordList = createStructuredRecordWithVerticalSchema(summary, stringStats,
				percentileScores, outputSchema, inputSchema, maxOccuringStringEntryList);
		return sparkExecutionPluginContext.getSparkContext().parallelize(recordList);
	}

	private List<StructuredRecord> createStructuredRecordWithVerticalSchema(MultivariateStatisticalSummary summary,
			List<List<Long>> stringStats, List<List<Double>> percentileScores, Schema outputSchema, Schema inputSchema,
			List<String> maxOccuringStringEntryList) {
		List<StructuredRecord.Builder> builderList = new LinkedList<StructuredRecord.Builder>();
		Schema verticalSchema = getVerticalFeaturesOutputSchema(inputSchema);
		for (Schema.Field field : inputSchema.getFields()) {
			builderList.add(StructuredRecord.builder(verticalSchema));
		}
		addFeatureNameRecord(inputSchema, outputSchema, builderList);
		addNumericStatsVertically(inputSchema, outputSchema, summary.variance().toArray(),
				FeatureSTATS.Variance.getName(), builderList);
		addNumericStatsVertically(inputSchema, outputSchema, summary.max().toArray(), FeatureSTATS.Max.getName(),
				builderList);
		addNumericStatsVertically(inputSchema, outputSchema, summary.min().toArray(), FeatureSTATS.Min.getName(),
				builderList);
		addNumericStatsVertically(inputSchema, outputSchema, summary.mean().toArray(), FeatureSTATS.Mean.getName(),
				builderList);
		addNumericStatsVertically(inputSchema, outputSchema, summary.numNonzeros().toArray(),
				FeatureSTATS.NumOfNonZeros.getName(), builderList);
		addNumericStatsVertically(inputSchema, outputSchema, summary.normL1().toArray(), FeatureSTATS.NormL1.getName(),
				builderList);
		addNumericStatsVertically(inputSchema, outputSchema, summary.normL2().toArray(), FeatureSTATS.NormL2.getName(),
				builderList);
		addNumericStatsVertically(inputSchema, outputSchema, convertToPrimitiveDoubleArray(percentileScores.get(0)),
				FeatureSTATS.TwentyFivePercentile.getName(), builderList);
		addNumericStatsVertically(inputSchema, outputSchema, convertToPrimitiveDoubleArray(percentileScores.get(1)),
				FeatureSTATS.FiftyPercentile.getName(), builderList);
		addNumericStatsVertically(inputSchema, outputSchema, convertToPrimitiveDoubleArray(percentileScores.get(2)),
				FeatureSTATS.SeventyFivePercentile.getName(), builderList);
		addInterQuartilePercentile(inputSchema, outputSchema, convertToPrimitiveDoubleArray(percentileScores.get(0)),
				convertToPrimitiveDoubleArray(percentileScores.get(2)), FeatureSTATS.InterQuartilePercentile.getName(),
				builderList);
		addStringStatsVertically(inputSchema, outputSchema, stringStats.get(0), FeatureSTATS.TotalCount.getName(),
				builderList);
		addStringStatsVertically(inputSchema, outputSchema, stringStats.get(1), FeatureSTATS.UniqueCount.getName(),
				builderList);
		addStringStatsVertically(inputSchema, outputSchema, stringStats.get(2),
				FeatureSTATS.LeastFrequentWordCount.getName(), builderList);
		addStringStatsVertically(inputSchema, outputSchema, stringStats.get(3),
				FeatureSTATS.MostFrequentWordCount.getName(), builderList);
		addMaxOccuringWordVertically(inputSchema, outputSchema, maxOccuringStringEntryList,
				FeatureSTATS.MostFrequentEntry.getName(), builderList);
		List<StructuredRecord> recordList = new LinkedList<>();
		for (Builder builder : builderList) {
			recordList.add(builder.build());
		}
		return recordList;
	}

	private void addFeatureNameRecord(Schema inputSchema, Schema outputSchema, List<Builder> builderList) {
		int index = 0;
		for (Schema.Field field : inputSchema.getFields()) {
			Builder builder = builderList.get(index++);
			builder.set(FeatureSTATS.Feature.getName(), field.getName());
			builder.set("Id", (long)index);
		}
	}

	private List<StructuredRecord> createStructuredRecord(MultivariateStatisticalSummary summary,
			List<List<Long>> stringStats, List<List<Double>> percentileScores, Schema outputSchema,
			Schema inputSchema) {
		List<StructuredRecord> recordList = new LinkedList<StructuredRecord>();

		StructuredRecord record = addNumericStat(inputSchema, outputSchema, summary.variance().toArray(), "Variance");
		recordList.add(record);

		record = addNumericStat(inputSchema, outputSchema, summary.max().toArray(), "max");
		recordList.add(record);

		record = addNumericStat(inputSchema, outputSchema, summary.min().toArray(), "Min");
		recordList.add(record);

		record = addNumericStat(inputSchema, outputSchema, summary.mean().toArray(), "Mean");
		recordList.add(record);

		record = addNumericStat(inputSchema, outputSchema, summary.numNonzeros().toArray(), "NumOfNonZeros");
		recordList.add(record);

		record = addNumericStat(inputSchema, outputSchema, summary.normL1().toArray(), "NormL1");
		recordList.add(record);

		record = addNumericStat(inputSchema, outputSchema, summary.normL2().toArray(), "NormL2");
		recordList.add(record);

		record = addNumericStat(inputSchema, outputSchema, convertToPrimitiveDoubleArray(percentileScores.get(0)),
				"25 Percentile");
		recordList.add(record);

		record = addNumericStat(inputSchema, outputSchema, convertToPrimitiveDoubleArray(percentileScores.get(1)),
				"50 Percentile");
		recordList.add(record);

		record = addNumericStat(inputSchema, outputSchema, convertToPrimitiveDoubleArray(percentileScores.get(2)),
				"75 Percentile");
		recordList.add(record);

		record = addStringStat(inputSchema, outputSchema, stringStats.get(0), "TotalCount");
		recordList.add(record);
		record = addStringStat(inputSchema, outputSchema, stringStats.get(1), "UniqueCount");
		recordList.add(record);
		record = addStringStat(inputSchema, outputSchema, stringStats.get(2), "LeastFrequentWordCount");
		recordList.add(record);
		record = addStringStat(inputSchema, outputSchema, stringStats.get(3), "MostFrequentWordCount");
		recordList.add(record);

		return recordList;
	}

	private double[] convertToPrimitiveDoubleArray(List<Double> list) {
		double[] result = new double[list.size()];
		int index = 0;
		for (Double d : list) {
			result[index++] = d;
		}
		return result;
	}

	private StructuredRecord addStringStat(Schema inputSchema, Schema outputSchema, List<Long> values,
			String statName) {
		StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);

		int index = 0;
		int valueIndex = 0;
		List<Schema.Field> outputFields = outputSchema.getFields();
		List<Schema.Field> inputFields = inputSchema.getFields();
		builder.set(outputFields.get(index).getName(), statName);

		for (index = 0; index < inputFields.size(); index++) {
			Schema.Field field = inputFields.get(index);
			if (!getSchemaType(field.getSchema()).equals(Schema.Type.STRING))
				builder.set(outputFields.get(index + 1).getName(), null);
			else
				builder.set(outputFields.get(index + 1).getName(), (values.get(valueIndex++) * 1.0));
		}

		return builder.build();
	}

	private Schema.Type getSchemaType(Schema schema) {
		if(schema.getType().equals(Schema.Type.UNION)) {
			List<Schema> schemas = schema.getUnionSchemas();
			if(schemas.size()==2) {
				if(schemas.get(0).getType().equals(Schema.Type.NULL))
					return schemas.get(1).getType();
				else 
					return schemas.get(0).getType();
			}
			return schema.getType();
		} else 
			return schema.getType();
	}
	
	private void addStringStatsVertically(Schema inputSchema, Schema outputSchema, List<Long> values, String statName,
			List<Builder> builderList) {
		int index = 0;
		int valueIndex = 0;

		List<Schema.Field> inputFields = inputSchema.getFields();
		for (index = 0; index < inputFields.size(); index++) {
			Builder builder = builderList.get(index);
			Schema.Field field = inputFields.get(index);
			if (!getSchemaType(field.getSchema()).equals(Schema.Type.STRING))
				builder.set(statName, null);
			else
				builder.set(statName, (values.get(valueIndex++)));
		}
	}

	private void addMaxOccuringWordVertically(Schema inputSchema, Schema outputSchema,
			List<String> maxOccuringStringEntryList, String statName, List<Builder> builderList) {
		int index = 0;
		int valueIndex = 0;

		List<Schema.Field> inputFields = inputSchema.getFields();
		for (index = 0; index < inputFields.size(); index++) {
			Builder builder = builderList.get(index);
			Schema.Field field = inputFields.get(index);
			if (!getSchemaType(field.getSchema()).equals(Schema.Type.STRING))
				builder.set(statName, null);
			else
				builder.set(statName, maxOccuringStringEntryList.get(valueIndex++));
		}
	}

	private StructuredRecord addNumericStat(Schema inputSchema, Schema outputSchema, double[] values, String statName) {
		StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);

		int index = 0;
		int valueIndex = 0;
		List<Schema.Field> outputFields = outputSchema.getFields();
		builder.set(outputFields.get(index++).getName(), statName);

		for (Schema.Field field : inputSchema.getFields()) {
			if (getSchemaType(field.getSchema()).equals(Schema.Type.STRING)) {
				builder.set(outputFields.get(index++).getName(), null);
				continue;
			}
			builder.set(outputFields.get(index++).getName(), values[valueIndex++]);
		}

		return builder.build();
	}

	private void addNumericStatsVertically(Schema inputSchema, Schema outputSchema, double[] values, String statName,
			List<Builder> builderList) {
		int index = 0;
		int valueIndex = 0;
		for (Schema.Field field : inputSchema.getFields()) {
			Builder builder = builderList.get(index++);
			if (getSchemaType(field.getSchema()).equals(Schema.Type.STRING)) {
				builder.set(statName, null);
			} else {
				builder.set(statName, values[valueIndex++]);
			}
		}
	}

	private void addInterQuartilePercentile(Schema inputSchema, Schema outputSchema, double[] value1, double[] value2,
			String statName, List<Builder> builderList) {
		int index = 0;
		int valueIndex = 0;
		for (Schema.Field field : inputSchema.getFields()) {
			Builder builder = builderList.get(index++);
			if (getSchemaType(field.getSchema()).equals(Schema.Type.STRING)) {
				builder.set(statName, null);
			} else {
				builder.set(statName, value2[valueIndex] - value1[valueIndex]);
				valueIndex++;
			}
		}
	}

	private List<List<Long>> getStringStats(List<List<String>> stringListRDD, List<Field> inputField,
			List<String> maxOccuringStringEntryList) throws InterruptedException, ExecutionException {
		List<List<Long>> stringStats = new LinkedList<>();// 0 = totalCount, 1 = uniqueCount, 2 = top word, 3 = top
															// word count
		for (int i = 0; i < 4; i++) {
			stringStats.add(new LinkedList<Long>());
		}

		for (int i = 0; i < stringListRDD.get(0).size(); i++) {
			Map<String, Long> wordCountMap = new HashMap<>();
			for (int j = 0; j < stringListRDD.size(); j++) {
				String key = stringListRDD.get(j).get(i);
				Long val = wordCountMap.get(key);
				if (val == null) {
					wordCountMap.put(key, 1L);
				} else
					wordCountMap.put(key, val + 1);
			}
			long noOfUniqueWords = wordCountMap.size();
			long mostFrequentWordCount = 0;
			long leastFrequentWordCount = Long.MAX_VALUE;
			long totalCount = 0;
			String maxOccuringWord = "";
			for (Map.Entry<String, Long> entry : wordCountMap.entrySet()) {
				if (entry.getValue() > mostFrequentWordCount) {
					mostFrequentWordCount = entry.getValue();
					maxOccuringWord = entry.getKey();
				}
				leastFrequentWordCount = Math.min(leastFrequentWordCount, entry.getValue());
				totalCount += entry.getValue();
			}
			maxOccuringStringEntryList.add(maxOccuringWord);
			stringStats.get(0).add(totalCount);
			stringStats.get(1).add(noOfUniqueWords);
			stringStats.get(2).add(leastFrequentWordCount);
			stringStats.get(3).add(mostFrequentWordCount);
		}
		return stringStats;
	}

	private JavaRDD<List<Double>> getDoubleListRDD(JavaRDD<StructuredRecord> javaRDD, final List<Field> inputField) {
		return javaRDD.map(new Function<StructuredRecord, List<Double>>() {

			@Override
			public List<Double> call(StructuredRecord record) throws Exception {
				List<Double> values = new LinkedList<Double>();
				for (Schema.Field field : inputField) {
					if (!getSchemaType(field.getSchema()).equals(Schema.Type.STRING)) {
						Object val = record.get(field.getName());
						if (val == null) {
							values.add(0.0);
							continue;
						}
						if (getSchemaType(field.getSchema()).equals(Schema.Type.BOOLEAN)) {
							val = val.toString().equals("true") ? 1 : 0;
						}
						try {
							values.add(Double.parseDouble(val.toString()));
						} catch (Exception e) {
							values.add(0.0);
						}
					}
				}
				return values;
			}
		});
	}

	private JavaRDD<Vector> getVectorRDD(JavaRDD<StructuredRecord> javaRDD, final List<Field> inputField) {
		return javaRDD.map(new Function<StructuredRecord, Vector>() {

			@Override
			public Vector call(StructuredRecord record) throws Exception {
				List<Double> values = new LinkedList<Double>();
				for (Schema.Field field : inputField) {
					if (!getSchemaType(field.getSchema()).equals(Schema.Type.STRING)) {
						Object val = record.get(field.getName());
						if (val == null) {
							values.add(0.0);
							continue;
						}
						if (getSchemaType(field.getSchema()).equals(Schema.Type.BOOLEAN)) {
							val = val.toString().equals("true") ? 1 : 0;
						}
						try {
							values.add(Double.parseDouble(val.toString()));
						} catch (Exception e) {
							values.add(0.0);
						}
					}
				}
				double[] valDouble = new double[values.size()];
				int index = 0;
				for (Double val : values) {
					valDouble[index++] = val;
				}
				return Vectors.dense(valDouble);
			}
		});
	}

	private Schema getInputSchema(JavaRDD<StructuredRecord> javaRDD) {
		Schema schema = javaRDD.first().getSchema();
		Map<String, Field> fieldMap = javaRDD.map(new Function<StructuredRecord, Map<String, Schema.Field>>() {

			@Override
			public Map<String, Field> call(StructuredRecord arg0) throws Exception {
				Map<String, Field> map = new HashMap<>();
				for (Field field : arg0.getSchema().getFields()) {
					map.put(field.getName(), field);
				}
				return map;
			}
		}).reduce(new Function2<Map<String, Field>, Map<String, Field>, Map<String, Field>>() {

			@Override
			public Map<String, Field> call(Map<String, Field> arg0, Map<String, Field> arg1) throws Exception {
				Map<String, Field> map = new HashMap<>();
				map.putAll(arg0);
				map.putAll(arg1);
				return map;
			}
		});
		return Schema.recordOf(schema.getRecordName(), new LinkedList<Field>(fieldMap.values()));
	}
}
