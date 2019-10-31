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

package co.cask.hydrator.plugin.flint

import java.util.concurrent.TimeUnit

import com.twosigma.flint.timeseries.TimeSeriesRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.types.StructType

import collection.JavaConverters._



object TimeSeriesRDDUtils {

  def fromRDD(
               rdd: RDD[Row],
               schema: StructType,
               isSorted: Boolean,
               timeUnit: TimeUnit,
               timeColumn: String
             ): TimeSeriesRDD = {

    TimeSeriesRDD.fromRDD(rdd, schema)(isSorted, timeUnit, timeColumn)
  }


  def toRow(vals: java.util.List[Object], structType: StructType): Row = {

    val seq = vals.asScala
    new GenericRowWithSchema(seq.toArray[Any], structType)
  }


}
