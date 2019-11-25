/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.plugin.EndpointPluginContext;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.common.LineageRecorder;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.common.batch.JobUtils;
import co.cask.hydrator.format.FileFormat;
import co.cask.hydrator.format.RegexPathFilter;
import co.cask.hydrator.format.input.CombinePathTrackingInputFormat;
import co.cask.hydrator.format.input.EmptyInputFormat;
import co.cask.hydrator.format.input.PathTrackingInputFormat;
import co.cask.hydrator.format.input.TextInputProvider;
import co.cask.hydrator.format.plugin.AbstractFileSource;
import co.cask.hydrator.format.plugin.FileSinkProperties;
import co.cask.hydrator.format.plugin.FileSourceProperties;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.ws.rs.Path;


/**
 * A {@link BatchSource} to use any distributed file system as a Source.
 */
@Plugin(type = "batchsource")
@Name("File")
@Description("Batch source for File Systems")
public class FileBatchSource extends AbstractFileSource<FileSourceConfig> {
  public static final Schema DEFAULT_SCHEMA = TextInputProvider.getDefaultSchema(null);
  static final String INPUT_NAME_CONFIG = "input.path.name";
  static final String INPUT_REGEX_CONFIG = "input.path.regex";
  static final String LAST_TIME_READ = "last.time.read";
  static final String CUTOFF_READ_TIME = "cutoff.read.time";
  static final String USE_TIMEFILTER = "timefilter";
  private static final Gson GSON = new Gson();
  private static final Type ARRAYLIST_DATE_TYPE = new TypeToken<ArrayList<Date>>() {
  }.getType();
  private final FileSourceConfig config;

  public FileBatchSource(FileSourceConfig config) {
    super(config);
    this.config = config;
  }

  private static String encryptId(String referenceId) {
    if (referenceId.startsWith("/")) {
      referenceId = referenceId.replaceFirst("/", "file_");
    }
    return referenceId.replace("/", "_--_").replace(":", "-__-").replace(".", "-___-")
        .replace("*", "_---_");
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    config.validate();
    config.setReferenceName(encryptId(config.getPath()));
    super.prepareRun(context);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    ((FileSourceProperties) this.config).validate();
    config.setReferenceName(encryptId(config.getPath()));
    Map<String, String> pipelineproperties = new HashMap<>(config.getProperties().getProperties());
    pipelineproperties.put("referenceName", config.getReferenceName());
    pipelineConfigurer.createDataset(config.getReferenceName(), Constants.EXTERNAL_DATASET_TYPE,
        DatasetProperties.builder()
            .add(DatasetProperties.SCHEMA, config.getSchema().toString())
            .addAll(pipelineproperties).build());

    Schema schema = config.getSchema();
    FileFormat fileFormat = config.getFormat();
    if (fileFormat != null) {
      fileFormat.getFileInputFormatter(config.getProperties().getProperties(), schema);
    }

    String pathField = config.getPathField();
    if (pathField != null && schema != null) {
      Schema.Field schemaPathField = schema.getField(pathField);
      if (schemaPathField == null) {
        throw new IllegalArgumentException(String.format("Path field '%s' is not present in the schema." +
                " Please add it to the schema as a string field.",
                pathField));
      }
      Schema pathFieldSchema = schemaPathField.getSchema();
      Schema.Type pathFieldType = pathFieldSchema.isNullable() ? pathFieldSchema.getNonNullable().getType() :
          pathFieldSchema.getType();
      if (pathFieldType != Schema.Type.STRING) {
        throw new IllegalArgumentException(
            String.format("Path field '%s' must be of type 'string', but found '%s'.", pathField, pathFieldType));
      }
    }
    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSchema());

  }

  private String getPath() {
    String path;
    if (config.getPath().startsWith("/")) {
      path = config.getPath().replaceFirst("/", "file_");
    } else {
      path = config.getPath();
    }
    return path;
  }

  /**
   * Endpoint method to get the output schema of a source.
   *
   * @param config        configuration for the source
   * @param pluginContext context to create plugins
   * @return schema of fields
   */
  @Path("getSchema")
  public Schema getSchema(FileSourceConfig config, EndpointPluginContext pluginContext) {
    FileFormat fileFormat = config.getFormat();
    if (fileFormat == null) {
      return config.getSchema();
    }
    Schema schema = fileFormat.getSchema(config.getPathField());
    return schema == null ? config.getSchema() : schema;
  }

  @Override
  protected Map<String, String> getFileSystemProperties(BatchSourceContext context) {
    Map<String, String> properties = new HashMap<>(config.getFileSystemProperties());
    if (config.shouldCopyHeader()) {
      properties.put(PathTrackingInputFormat.COPY_HEADER, "true");
    }

    // TODO:(CDAP-14424) Remove time table logic
    // everything from this point on should be removed in a future release.
    // the time table stuff is super specific, requiring input paths to be in a very specific format
    // and it assumes the pipeline is scheduled to run in a specific way

    //SimpleDateFormat needs to be local because it is not threadsafe
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH");

    //calculate date one hour ago, rounded down to the nearest hour
    Date prevHour = new Date(context.getLogicalStartTime() - TimeUnit.HOURS.toMillis(1));
    Calendar cal = Calendar.getInstance();
    cal.setTime(prevHour);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    prevHour = cal.getTime();

    if (config.getTimeTable() != null) {
      KeyValueTable table = context.getDataset(config.getTimeTable());
      String datesToRead = Bytes.toString(table.read(LAST_TIME_READ));
      if (datesToRead == null) {
        List<Date> firstRun = new ArrayList<>(1);
        firstRun.add(new Date(0));
        datesToRead = GSON.toJson(firstRun, ARRAYLIST_DATE_TYPE);
      }
      List<Date> attempted = new ArrayList<>();
      attempted.add(prevHour);
      String updatedDatesToRead = GSON.toJson(attempted, ARRAYLIST_DATE_TYPE);
      if (!updatedDatesToRead.equals(datesToRead)) {
        table.write(LAST_TIME_READ, updatedDatesToRead);
      }
      properties.put(LAST_TIME_READ, datesToRead);
    }

    properties.put(CUTOFF_READ_TIME, dateFormat.format(prevHour));
    Pattern pattern = config.getFilePattern();
    properties.put(INPUT_REGEX_CONFIG, pattern == null ? ".*" : pattern.toString());
    properties.put("mapreduce.input.pathFilter.class", BatchFileFilter.class.getName());

    return properties;
  }
}
