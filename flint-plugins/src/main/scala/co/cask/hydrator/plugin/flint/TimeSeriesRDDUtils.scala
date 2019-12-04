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
