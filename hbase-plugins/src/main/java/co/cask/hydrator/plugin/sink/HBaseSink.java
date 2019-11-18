/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.hydrator.plugin.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.format.RecordPutTransformer;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.common.KeyValueListParser;
import co.cask.hydrator.common.LineageRecorder;
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.SchemaValidator;
import co.cask.hydrator.common.batch.JobUtils;
import co.cask.hydrator.plugin.HBaseConfig;
import co.cask.hydrator.plugin.sink.mapreduce.HBaseTableOutputFormat;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.mapreduce.KeyValueSerialization;
import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Sink to write to HBase tables.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("HBase")
@Description("HBase Batch Sink")
public class HBaseSink extends ReferenceBatchSink<StructuredRecord, NullWritable, Mutation> {

  private HBaseSinkConfig config;
  private RecordPutTransformer recordPutTransformer;

  public HBaseSink(HBaseSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    config.referenceName = config.tableName;
    Job job;
    ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
    // Switch the context classloader to plugin class' classloader (PluginClassLoader) so that
    // when Job/Configuration is created, it uses PluginClassLoader to load resources (hbase-default.xml)
    // which is present in the plugin jar and is not visible in the CombineClassLoader (which is what oldClassLoader
    // points to).
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    try {
      job = JobUtils.createInstance();
    } finally {
      // Switch back to the original
      Thread.currentThread().setContextClassLoader(oldClassLoader);
    }

    Configuration conf = job.getConfiguration();
    HBaseConfiguration.addHbaseResources(conf);
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    lineageRecorder.createExternalDataset(config.getSchema());
    context.addOutput(Output.of(config.referenceName, new HBaseOutputFormatProvider(config, conf)));
    context.getDataset(config.referenceName);
  }

  public Map<String, String> getAdditionalProperties() {
    KeyValueListParser kvParser = new KeyValueListParser("\\s*;\\s*", ",");
    Map<String, String> conf = new HashMap<>();
    if (!Strings.isNullOrEmpty(config.addProperties)) {
      for (KeyValue<String, String> keyVal : kvParser
              .parse(config.addProperties)) {
        conf.put(keyVal.getKey(), keyVal.getValue());
      }
    }
    return conf;
  }
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.referenceName = config.tableName;
    pipelineConfigurer.createDataset(config.referenceName, Constants.EXTERNAL_DATASET_TYPE,
            DatasetProperties.builder()
                    .add(DatasetProperties.SCHEMA, pipelineConfigurer.getStageConfigurer().getInputSchema().toString())
                    .addAll(config.getProperties().getProperties()).build());
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.rowField),
                                "Row field must be given as a property.");
    Schema outputSchema =
      SchemaValidator.validateOutputSchemaAndInputSchemaIfPresent(config.schema,
                                                                  config.rowField, pipelineConfigurer);
    // NOTE: this is done only for testing, once CDAP-4575 is implemented, we can use this schema in initialize
    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  private class HBaseOutputFormatProvider implements OutputFormatProvider {

    private final Map<String, String> conf;

    HBaseOutputFormatProvider(HBaseSinkConfig config, Configuration configuration) {
      this.conf = new HashMap<>();
      Map<String, String> addProp = getAdditionalProperties();
      conf.put(TableOutputFormat.OUTPUT_TABLE, config.tableName);
      String zkQuorum = !Strings.isNullOrEmpty(config.zkQuorum) ? config.zkQuorum : "localhost";
      String zkClientPort = !Strings.isNullOrEmpty(config.zkClientPort) ? config.zkClientPort : "2181";
      String zkNodeParent = !Strings.isNullOrEmpty(config.zkNodeParent) ? config.zkNodeParent : "/hbase";
      conf.put(TableOutputFormat.QUORUM_ADDRESS, String.format("%s:%s:%s", zkQuorum, zkClientPort, zkNodeParent));
      conf.putAll(addProp);
      String[] serializationClasses = {
        configuration.get("io.serializations"),
        MutationSerialization.class.getName(),
        ResultSerialization.class.getName(),
        KeyValueSerialization.class.getName() };
      conf.put("io.serializations", StringUtils.arrayToString(serializationClasses));
    }

    @Override
    public String getOutputFormatClassName() {
      return HBaseTableOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    // If a schema string is present in the properties, use that to construct the outputSchema and pass it to the
    // recordPutTransformer
    recordPutTransformer = new RecordPutTransformer(config.rowField, config.getSchema());
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Mutation>> emitter) throws Exception {
    Put put = recordPutTransformer.toPut(input);
    org.apache.hadoop.hbase.client.Put hbasePut = new org.apache.hadoop.hbase.client.Put(put.getRow());
    for (Map.Entry<byte[], byte[]> entry : put.getValues().entrySet()) {
      hbasePut.add(config.columnFamily.getBytes(), entry.getKey(), entry.getValue());
    }
    emitter.emit(new KeyValue<NullWritable, Mutation>(NullWritable.get(), hbasePut));
  }

  /**
   * HBaseSink plugin.
   */
  public static class HBaseSinkConfig extends HBaseConfig {
    @Description("Parent Node of HBase in Zookeeper. Defaults to '/hbase'")
    @Nullable
    private String zkNodeParent;

    @Nullable
    @Name("addProperties")
    @Description("Additional properties that the user may want to specify.")
    private String addProperties;

    public HBaseSinkConfig(String tableName, String rowField, @Nullable String schema) {
      super(String.format("HBase_%s", tableName), tableName, rowField, schema);
    }

    public HBaseSinkConfig(String referenceName, String tableName, String rowField, @Nullable String schema) {
      super(referenceName, tableName, rowField, schema);
    }
  }
}
