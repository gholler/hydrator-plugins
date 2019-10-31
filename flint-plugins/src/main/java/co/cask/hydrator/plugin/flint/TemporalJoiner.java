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

package co.cask.hydrator.plugin.flint;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.MultiInputPipelineConfigurer;
import co.cask.cdap.etl.api.MultiInputStageConfigurer;
import co.cask.cdap.etl.api.batch.BatchJoinerContext;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkJoiner;
import co.cask.cdap.etl.api.lineage.field.FieldOperation;
import co.cask.cdap.etl.api.lineage.field.FieldTransformOperation;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.twosigma.flint.timeseries.TimeSeriesRDD;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Row$;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.concurrent.duration.Duration;

import javax.ws.rs.Path;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Plugin(type = SparkJoiner.PLUGIN_TYPE)
@Name("TemporalJoiner")
@Description("Perform temporal left outer join operation on records from exactly two input stages. " +
    "Bot input stages are expected to have a time field on which the temporal part of the join " +
    "will be executed with unexact match semantic: for each record on the left part of the join, " +
    "the most recent previous record of the right part of the join will be merged if the duration between both records " +
    "falls behind a given tolerance 'e.g. 10 minutes). Optionnaly, other join keys with exact match semantic may also" +
    "be provided, so that only the most recent record on the right side that can be joined via exact match will be considered ")
public class TemporalJoiner extends SparkJoiner<StructuredRecord> {

  private static Logger log = LoggerFactory.getLogger(TemporalJoiner.class);

  public static final String JOIN_OPERATION_DESCRIPTION = "Used as a key in a join";
  public static final String IDENTITY_OPERATION_DESCRIPTION = "Unchanged as part of a join";
  public static final String RENAME_OPERATION_DESCRIPTION = "Renamed as a part of a join";

  public static final String TIMEFIELD_ALIAS = "$$time$$";
  public static final String JOIN_KEY_ALIAS = "$$key$$";
  private final TemporalJoinerConfig conf;

  public TemporalJoiner(TemporalJoinerConfig conf) {
    this.conf = conf;
  }

  private Map<String, Schema> inputSchemas;
  private Schema outputSchema;
  private Map<String, List<String>> perStageJoinKeys;
  private Table<String, String, String> perStageSelectedFields;
  private Multimap<String, String> duplicateFields = ArrayListMultimap.create();


  @Override
  public void initialize(SparkExecutionPluginContext context) throws Exception {
    log.debug("initializing TemporalJoiner");
    init(context.getInputSchemas());
    inputSchemas = context.getInputSchemas();
    outputSchema = context.getOutputSchema();
  }

  private void init(Map<String, Schema> inputSchemas) {
    validateTemporalFields(inputSchemas, conf);
    validateJoinKeySchemas(inputSchemas, conf.getPerStageJoinKeys());
    validateFieldNames(inputSchemas);
    validateTolerance(conf.getTolerance());
    perStageSelectedFields = conf.getPerStageSelectedFields();
  }

  private void validateFieldNames(Map<String, Schema> inputSchemas) throws IllegalArgumentException {
    for (Schema schema : inputSchemas.values()) {

      schema.getFields().forEach(field -> {
        if (field.getName().startsWith("$")) {
          throw new IllegalArgumentException(String.format("Field '%s' is starting with character '$' which is not allowed",
              field.getName()));
        }
      });
    }
  }

  private void validateTolerance(String tolerance) throws IllegalArgumentException{
    try {
      Duration.apply(tolerance);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(String.format("Illegal tolerance : %s", tolerance), e);
    }
  }

  private void validateTemporalFields(Map<String, Schema> inputSchemas, TemporalJoinerConfig conf) throws  IllegalArgumentException{

    if (inputSchemas.size() != 2) {
      throw new IllegalArgumentException(String.format("Found %s inputs, exactly 2 is required ", inputSchemas.size()));
    }

    Schema leftSchema = null;
    Schema rightSchema = null;
    for (Map.Entry<String, Schema> entry : inputSchemas.entrySet()) {
      if (entry.getKey().equals(conf.leftInput)) {
        leftSchema = entry.getValue();
      } else {
        rightSchema = entry.getValue();
      }
    }
    assert leftSchema != null && rightSchema != null;

    validateTimeField(conf.getTimeFieldLeft(), leftSchema);
    validateTimeField(conf.getTimeFieldRight(), rightSchema);

  }

  private void validateTimeField(String timeFieldName, Schema schema) throws IllegalArgumentException{
    Schema.Field timeField = schema.getField(timeFieldName);
    if (timeField == null) {
      throw new IllegalArgumentException(String.format("Unknown time field %s for left stage", timeFieldName));
    }
    if (!(timeField.getSchema().getType() == Schema.Type.LONG || timeField.getSchema().getType() == Schema.Type.INT)) {
      throw new IllegalArgumentException(String.format("Expected integer type for field %s. Found %s", timeFieldName, timeField.getSchema().getType()));
    }
  }

  void validateJoinKeySchemas(Map<String, Schema> inputSchemas, Map<String, List<String>> joinKeys) {
    perStageJoinKeys = joinKeys;

    if (perStageJoinKeys.size() != inputSchemas.size()) {
      throw new IllegalArgumentException("There should be join keys present from each stage");
    }

    List<Schema> prevSchemaList = null;
    for (Map.Entry<String, List<String>> entry : perStageJoinKeys.entrySet()) {
      ArrayList<Schema> schemaList = new ArrayList<>();
      String stageName = entry.getKey();

      Schema schema = inputSchemas.get(stageName);
      if (schema == null) {
        throw new IllegalArgumentException(String.format("Input schema for input stage %s can not be null", stageName));
      }

      for (String joinKey : entry.getValue()) {
        Schema.Field field = schema.getField(joinKey);
        if (field == null) {
          throw new IllegalArgumentException(String.format("Join key field %s is not present in input of stage %s",
              joinKey, stageName));
        }
        schemaList.add(field.getSchema().isNullable()? field.getSchema().getNonNullable(): field.getSchema());
      }
      if (prevSchemaList != null && !prevSchemaList.equals(schemaList)) {
        throw new IllegalArgumentException(String.format("For stage %s, Schemas of joinKeys %s are expected to be: " +
                "%s, but found: %s",
            stageName, entry.getValue(), prevSchemaList.toString(),
            schemaList.toString()));
      }
      prevSchemaList = schemaList;
    }
  }




  @Override
  public void configurePipeline(MultiInputPipelineConfigurer pipelineConfigurer) {
    log.debug("Configuring pipeline");
    MultiInputStageConfigurer stageConfigurer = pipelineConfigurer.getMultiInputStageConfigurer();
    Map<String, Schema> inputSchemas = stageConfigurer.getInputSchemas();
    init(inputSchemas);
    //validate the input schema and get the output schema for it
    stageConfigurer.setOutputSchema(getOutputSchema(conf.leftInput, inputSchemas));
  }


  @Override
  public void prepareRun(BatchJoinerContext context) throws Exception {
    log.debug("prepare run");
    if (conf.getNumPartitions() != null) {
      context.setNumPartitions(conf.getNumPartitions());
    }
    init(context.getInputSchemas());
    Collection<OutputFieldInfo> outputFieldInfos = createOutputFieldInfos(conf.leftInput, context.getInputSchemas());
    context.record(createFieldOperations(outputFieldInfos, perStageJoinKeys));
  }

  /**
   * Create the field operations from the provided OutputFieldInfo instances and join keys.
   * For join we record several types of transformation; Join, Identity, and Rename.
   * For each of these transformations, if the input field is directly coming from the schema
   * of one of the stage, the field is added as {@code stage_name.field_name}. We keep track of fields
   * outputted by operation (in {@code outputsSoFar set}, so that any operation uses that field as
   * input later, we add it without the stage name.
   *
   * Join transform operation is added with join keys as input tagged with the stage name, and join keys
   * without stage name as output.
   *
   * For other fields which are not renamed in join, Identity transform is added, while for fields which
   * are renamed Rename transform is added.
   *
   * @param outputFieldInfos collection of output fields along with information such as stage name, alias
   * @param perStageJoinKeys join keys
   * @return List of field operations
   */
  @VisibleForTesting
  public static List<FieldOperation> createFieldOperations(Collection<OutputFieldInfo> outputFieldInfos,
                                                           Map<String, List<String>> perStageJoinKeys) {
    LinkedList<FieldOperation> operations = new LinkedList<>();

    // Add JOIN operation
    List<String> joinInputs = new ArrayList<>();
    Set<String> joinOutputs = new LinkedHashSet<>();
    for (Map.Entry<String, List<String>> joinKey : perStageJoinKeys.entrySet()) {
      for (String field : joinKey.getValue()) {
        joinInputs.add(joinKey.getKey() + "." + field);
        joinOutputs.add(field);
      }
    }
    FieldOperation joinOperation = new FieldTransformOperation("Join", JOIN_OPERATION_DESCRIPTION, joinInputs,
        new ArrayList<>(joinOutputs));
    operations.add(joinOperation);

    Set<String> outputsSoFar = new HashSet<>(joinOutputs);

    for (OutputFieldInfo outputFieldInfo : outputFieldInfos) {
      // input field name for the operation will come in from schema if its not outputted so far
      String stagedInputField = outputsSoFar.contains(outputFieldInfo.inputFieldName) ?
          outputFieldInfo.inputFieldName : outputFieldInfo.stageName + "." + outputFieldInfo.inputFieldName;

      if (outputFieldInfo.name.equals(outputFieldInfo.inputFieldName)) {
        // Record identity transform
        if (perStageJoinKeys.get(outputFieldInfo.stageName).contains(outputFieldInfo.inputFieldName)) {
          // if the field is part of join key no need to emit the identity transform as it is already taken care
          // by join
          continue;
        }
        String operationName = String.format("Identity %s", stagedInputField);
        FieldOperation identity = new FieldTransformOperation(operationName, IDENTITY_OPERATION_DESCRIPTION,
            Collections.singletonList(stagedInputField),
            outputFieldInfo.name);
        operations.add(identity);
        continue;
      }

      String operationName = String.format("Rename %s", stagedInputField);

      FieldOperation transform = new FieldTransformOperation(operationName, RENAME_OPERATION_DESCRIPTION,
          Collections.singletonList(stagedInputField),
          outputFieldInfo.name);
      operations.add(transform);
    }

    return operations;
  }

  Schema getOutputSchema(String leftInput, Map<String, Schema> inputSchemas) {
    return Schema.recordOf("join.output", getOutputFields(createOutputFieldInfos(leftInput, inputSchemas)));
  }

  private List<Schema.Field> getOutputFields(Collection<OutputFieldInfo> fieldsInfo) {
    List<Schema.Field> outputFields = new ArrayList<>();
    for (OutputFieldInfo fieldInfo : fieldsInfo) {
      outputFields.add(fieldInfo.getField());
    }
    return outputFields;
  }


  @Override
  public JavaRDD<StructuredRecord> join(SparkExecutionPluginContext context, Map<String, JavaRDD<?>> inputs) throws Exception {


    StageInfo leftStageInfo = null;
    StageInfo rightStageInfo = null;

    // 1 - get stage info

    for (Map.Entry<String, Schema> entry : inputSchemas.entrySet()) {
      String stageName = entry.getKey();
      Schema inputSchema = entry.getValue();
      Map<String, String> colToAlias = perStageSelectedFields.row(stageName);
      Map<String, String> joinKeyToAlias = getKeyToAlias(stageName, perStageJoinKeys);
      JavaRDD<StructuredRecord> rdd = (JavaRDD<StructuredRecord>) inputs.get(stageName);
      if (log.isDebugEnabled()) {
        log.debug("Count for stage {}: {}", stageName, rdd.count());
      }
      if (conf.leftInput.equals(stageName)) {
        leftStageInfo = new StageInfo(stageName, rdd, inputSchema, conf.timeFieldLeft, colToAlias, joinKeyToAlias);

      } else {
        rightStageInfo = new StageInfo(stageName, rdd, inputSchema, conf.timeFieldRight, colToAlias, joinKeyToAlias);
      }
    }

    assert leftStageInfo != null && rightStageInfo != null;



    // 1 - transform inputs to time series with target schema
    TimeSeriesRDD leftTimeSeriesRDD = asTimeSeriesRDD((JavaRDD<StructuredRecord>) leftStageInfo
        .rdd.map(leftStageInfo.getTransformInput()), leftStageInfo.getTransformedSchema(), false);
    TimeSeriesRDD rightTimeSeriesRDD = asTimeSeriesRDD((JavaRDD<StructuredRecord>) rightStageInfo
        .rdd.map(rightStageInfo.getTransformInput()), rightStageInfo.getTransformedSchema(), true);

    // 2 - temporal join
    Seq<String> keys = JavaConverters.asScalaIteratorConverter(leftStageInfo.keyToAlias.values().iterator()).asScala().toSeq();

    TimeSeriesRDD joinedTimeSeriesRDD = leftTimeSeriesRDD.leftJoin(rightTimeSeriesRDD, conf.tolerance, keys, "", "");


    log.debug("resulting schema for joinedTimeSeriesRDD: {}",joinedTimeSeriesRDD.schema());


    // 3 - transform to final format
    JavaRDD<StructuredRecord> result = (JavaRDD<StructuredRecord>) joinedTimeSeriesRDD.rdd().toJavaRDD()
        .map(new RowToStructuredRecordFunction(outputSchema));
    if (log.isDebugEnabled()) {
      log.debug("result count: {}",result.count());
    }
    return result;

  }

  private static Map<String, String> getKeyToAlias(String stageName, Map<String, List<String>> perStageJoinKeys) {
    List<String> joinKeys = perStageJoinKeys.get(stageName);
    Map<String, String> joinKeyToAlias = new HashMap<>();
    int i=0;
    for (String key : joinKeys) {
      String alias = JOIN_KEY_ALIAS + i;
      joinKeyToAlias.put(key, alias);
      i++;
    }
    return joinKeyToAlias;
  }


  private static class StageInfo implements Externalizable {
    private String stageName;
    private JavaRDD<StructuredRecord> rdd;
    private Schema inputSchema;
    private String timeField;
    private Map<String, String> colToAlias;
    private Map<String, String> keyToAlias;

    //for serialization only
    public StageInfo() {
    }

    public StageInfo(String stageName, JavaRDD<StructuredRecord> rdd, Schema inputSchema, String timeField, Map<String, String> colToAlias, Map<String, String> keyToAlias) {
      Preconditions.checkNotNull(stageName);
      Preconditions.checkNotNull(rdd);
      Preconditions.checkNotNull(inputSchema);
      Preconditions.checkNotNull(timeField);
      Preconditions.checkNotNull(colToAlias);
      Preconditions.checkNotNull(keyToAlias);

      this.stageName = stageName;
      this.rdd = rdd;
      this.inputSchema = inputSchema;
      this.timeField = timeField;
      this.colToAlias = colToAlias;
      this.keyToAlias = keyToAlias;

      transformInput = new TransformInput();
    }


    private Schema getTransformedSchema() {

      List<Schema.Field> fields = new ArrayList<>();

      inputSchema.getFields().forEach(field -> {
        String alias = colToAlias.get(field.getName());
        if (alias != null) {
          fields.add(Schema.Field.of(alias, field.getSchema()));
        }
      });

      fields.add(Schema.Field.of(TIMEFIELD_ALIAS, inputSchema.getField(timeField).getSchema()));

      keyToAlias.forEach((key,alias) ->{
        fields.add(Schema.Field.of(alias, inputSchema.getField(key).getSchema()));
      });

      return Schema.recordOf(stageName+"-transformed", fields);

    }


    private class TransformInput implements Function<StructuredRecord, StructuredRecord> {

      private final Schema transformedSchema = getTransformedSchema();

      @Override
      public StructuredRecord call(StructuredRecord record) throws Exception {
        log.trace("called with {}", record.toString());
        StructuredRecord.Builder builder = StructuredRecord.builder(transformedSchema);
        inputSchema.getFields().stream().map(Schema.Field::getName).forEach(name -> {
          String alias = colToAlias.get(name);
          if (alias != null) {
            builder.set(alias, record.get(name));
          }
        });
        builder.set(TIMEFIELD_ALIAS,record.get(timeField));
        keyToAlias.forEach((key, alias)-> {
          builder.set(alias, record.get(key));
        });
        return builder.build();
      }
    }

    private transient TransformInput transformInput;

    private TransformInput getTransformInput() { return transformInput;}

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeObject(stageName);
      out.writeObject(rdd);
      out.writeObject(inputSchema);
      out.writeObject(timeField);
      out.writeObject(colToAlias);
      out.writeObject(keyToAlias);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      stageName = (String) in.readObject();
      rdd = (JavaRDD<StructuredRecord>) in.readObject();
      inputSchema = (Schema) in.readObject();
      timeField = (String) in.readObject();
      colToAlias = (Map<String, String>) in.readObject();
      keyToAlias = (Map<String, String>)  in.readObject();

      transformInput = new TransformInput();
    }
  }

  @Path("outputSchema")
  @VisibleForTesting
  public Schema getOutputSchema(GetSchemaRequest request) {
    validateJoinKeySchemas(request.inputSchemas, request.getPerStageJoinKeys());
    perStageSelectedFields = request.getPerStageSelectedFields();
    duplicateFields = ArrayListMultimap.create();
    return getOutputSchema(request.leftInput, request.inputSchemas);
  }


  /**
   * Endpoint request for output schema.
   */
  public static class GetSchemaRequest extends TemporalJoinerConfig {
    public Map<String, Schema> inputSchemas;
  }


  private TimeSeriesRDD asTimeSeriesRDD(JavaRDD<StructuredRecord> rdd, Schema schema, boolean forceNullable) {
    StructType structType = asStructType(schema, forceNullable);

    JavaRDD<Row> rowRDD = (JavaRDD<Row>) rdd.map(new StruturedrecordToRowfunction(structType));


    // TODO expose isSorted and possibily timeUnit as plugin parameters
    return TimeSeriesRDDUtils.fromRDD(rowRDD.rdd(), structType, false, TimeUnit.MILLISECONDS, TIMEFIELD_ALIAS);
  }

  private static StructType asStructType(Schema schema, boolean forceNullable) {
    List<StructField> structFields = new ArrayList<>();
    schema.getFields().forEach((field) -> {
      Schema fieldSchema = field.getSchema();
      boolean nullable = fieldSchema.isNullable();
      fieldSchema = nullable? fieldSchema.getNonNullable() : fieldSchema;
      if (log.isDebugEnabled()) {
        log.debug("name: {} type: {} nullable: {}", field.getName(), fieldSchema.getType(), nullable);
      }
      // force nullable?
      nullable = forceNullable || nullable;
      structFields.add(new StructField(field.getName(), convert(fieldSchema.getType()), nullable, Metadata.empty()));
    });

    return new StructType(structFields.toArray(new StructField[0]));
  }

  private <T> T getOrElse(T v, T dft) {
    if (v instanceof String) {
      v = StringUtils.isEmpty((String) v) ? null : v;
    }
    return v == null ? dft : v;
  }

  private static DataType convert(Schema.Type type) {
    switch (type) {
      case BYTES:
        return DataTypes.BinaryType;
      case BOOLEAN:
        return DataTypes.BooleanType;
      case INT:
        return DataTypes.IntegerType;
      case LONG:
        return DataTypes.LongType;
      case FLOAT:
        return DataTypes.FloatType;
      case DOUBLE:
        return DataTypes.DoubleType;
      case STRING:
        return DataTypes.StringType;
      default:
        throw new IllegalArgumentException(String.format("Don't know how to convert %s to spark DataType", type));
    }
  }


  private static class StruturedrecordToRowfunction implements Function<StructuredRecord, Row> {

    private final StructType structType;


    public StruturedrecordToRowfunction(StructType structType) {
      this.structType = structType;
    }

    @Override
    public Row call(StructuredRecord v1) throws Exception {

      log.trace("v1: {}", v1);

      List<Object> values = new ArrayList<>(structType.fields().length);

      for (StructField field : structType.fields()) {
        values.add((v1.get(field.name())));
      }
      return TimeSeriesRDDUtils.toRow(values,structType);
    }
  }

  private static class RowToStructuredRecordFunction implements Function<Row, StructuredRecord> {

    private final Schema schema;

    public RowToStructuredRecordFunction(Schema schema) {
      this.schema = schema;
    }

    @Override
    public StructuredRecord call(Row row) throws Exception {
      log.trace("row: {}", row);
      StructuredRecord.Builder builder = StructuredRecord.builder(schema);
      // only fields from target schema are kept, so all synthetic fields beginning with $$ are purposely forgotten
      schema.getFields().forEach((field) -> {
        builder.set(field.getName(), row.getAs(flintName(field)));
      });
      return builder.build();
    }

    private String flintName(Schema.Field field) {
      String name = field.getName();
      if (!(name.startsWith("_"))) {
        name = "_"+name;
      }
      return name;
    }
  }


  private Collection<OutputFieldInfo> createOutputFieldInfos(String leftInput, Map<String, Schema> inputSchemas) {
    validateLeftInput(leftInput, inputSchemas);

    // stage name to input schema
    Map<String, Schema> inputs = new HashMap<>(inputSchemas);
    // Selected Field name to output field info
    Map<String, OutputFieldInfo> outputFieldInfo = new LinkedHashMap<>();
    List<String> duplicateAliases = new ArrayList<>();

    // order of fields in output schema will be same as order of selectedFields
    Set<Table.Cell<String, String, String>> rows = perStageSelectedFields.cellSet();
    for (Table.Cell<String, String, String> row : rows) {
      String stageName = row.getRowKey();
      String inputFieldName = row.getColumnKey();
      String alias = row.getValue();
      Schema inputSchema = inputs.get(stageName);

      if (inputSchema == null) {
        throw new IllegalArgumentException(String.format("Input schema for input stage %s can not be null", stageName));
      }

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
        throw new IllegalArgumentException(String.format(
            "Invalid field: %s of stage '%s' does not exist in input schema %s.",
            inputFieldName, stageName, inputSchema));
      }
      // set nullable fields for non-required inputs
      if (leftInput.equals(stageName) || inputField.getSchema().isNullable()) {
        outputFieldInfo.put(alias, new OutputFieldInfo(alias, stageName, inputFieldName,
            Schema.Field.of(alias, inputField.getSchema())));
      } else {
        outputFieldInfo.put(alias, new OutputFieldInfo(alias, stageName, inputFieldName,
            Schema.Field.of(alias,
                Schema.nullableOf(inputField.getSchema()))));
      }
    }

    if (!duplicateFields.isEmpty()) {
      throw new IllegalArgumentException(String.format("Output schema must not have any duplicate field names, but " +
              "found duplicate fields: %s for aliases: %s", duplicateFields,
          duplicateAliases));
    }

    return outputFieldInfo.values();
  }

  private void validateLeftInput(String leftInput, Map<String, Schema> inputSchemas) throws IllegalArgumentException {

    if (!inputSchemas.containsKey(leftInput)) {
      throw new IllegalArgumentException(String.format("Provided left input %s is not an input stage name.",
          leftInput));
    }
  }




  /**
   * Class to hold information about output fields
   */
  @VisibleForTesting
  static class OutputFieldInfo {
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
      return "OutputFieldInfo{" +
          "name='" + name + '\'' +
          ", stageName='" + stageName + '\'' +
          ", inputFieldName='" + inputFieldName + '\'' +
          ", field=" + field +
          '}';
    }
  }


}
