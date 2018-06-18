package uk.gov.ons.dsc.trade

import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class ShipProgressUDAF

  extends UserDefinedAggregateFunction {

  // the input field of the aggr function
  override def inputSchema: StructType =
    StructType (StructField("lat", IntegerType) :: StructField("long", IntegerType) :: Nil )

  // internal fields for computing the aggregate
  override def bufferSchema: StructType =
    StructType (StructField("RouteDist", IntegerType)  :: Nil )

  // output type of the aggregation function
  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  //initial value for the buffer schema
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
  }

  // updating the buffer schema given an input
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Double](0) + input.getDouble(0)+input.getDouble(1)
  }

  // merging two objects with bufferSchema type
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

  }

  //output the final value, given the final value of the bufferSchema
  override def evaluate(buffer: Row): Any = {
    buffer.getDouble(0)
  }

}


