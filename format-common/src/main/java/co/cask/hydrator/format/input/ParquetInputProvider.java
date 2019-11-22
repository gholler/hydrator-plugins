/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.hydrator.format.input;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.*;

import javax.annotation.Nullable;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;


/**
 * Provides Parquet formatters.
 */
public class ParquetInputProvider implements FileInputFormatterProvider {

  Gson gson = new Gson();
  @Nullable
  @Override
  public Schema getSchema(@Nullable String pathField) throws IOException {

      if (Strings.isNullOrEmpty(pathField)) {
          throw new IllegalArgumentException("Path Field should contain a valid path for fetching Schema");
      }
      ParquetReader<GenericData.Record> reader = null;
      Path path = new Path(pathField);
      try {
          ParquetMetadata readFooter = ParquetFileReader.readFooter(new Configuration(), path, NO_FILTER);
          MessageType mt = readFooter.getFileMetaData().getSchema();
          org.apache.avro.Schema schema = new AvroSchemaConverter().convert(mt);

          String jsonSchema = schema.toString();
          for(Schema.Type st : Schema.Type.values()) {
              if(st.isSimpleType() && !st.name().equals(Schema.Type.NULL.name())) {
                  jsonSchema = jsonSchema.replace("[\"null\",\"" + st.name().toLowerCase() + "\"]",
                          "[\"" + st.name().toLowerCase() + "\",\"null\"]" );
              }
          }

          System.out.println("Parquet CDAP Schema :" + Schema.parseJson(jsonSchema));

          return Schema.parseJson(jsonSchema);
      } catch (IOException e) {
          e.printStackTrace();
          throw new IOException("Exception occured while fetching Parquet Schema : " + e.getMessage());
      } finally {
          if (reader != null) {
              try {
                  reader.close();
              } catch (IOException e) {
                  e.printStackTrace();
              }
          }
      }
  }

  @Override
  public FileInputFormatter create(Map<String, String> properties, @Nullable Schema schema) {
    return new ParquetInputFormatter(schema);
  }
}
