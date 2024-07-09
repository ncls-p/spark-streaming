package org.example.spark.processor

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object DataProcessor {
  def readJson(spark: SparkSession, path: String): DataFrame = {
    spark.read.option("multiline", "true").json(path)
  }

  def processTrainSchedules(
      dfVehicleJourneys: DataFrame,
      dfStopPoints: DataFrame
  ): DataFrame = {
    val trainSchedules = dfVehicleJourneys
      .select(explode(col("vehicle_journeys")).as("vehicle_journey"))
      .select(
        col("vehicle_journey.id").alias("journey_id"),
        explode(col("vehicle_journey.stop_times")).as("stop_time")
      )
      .select(
        col("journey_id"),
        col("stop_time.stop_point.id").alias("stop_point_id"),
        col("stop_time.arrival_time"),
        col("stop_time.departure_time")
      )

    val stopPoints = dfStopPoints
      .select(explode(col("stop_points")).as("stop_point"))
      .select(
        col("stop_point.id").alias("stop_point_id"),
        col("stop_point.name").alias("stop_point_name"),
        col("stop_point.coord.lon").alias("longitude"),
        col("stop_point.coord.lat").alias("latitude")
      )

    trainSchedules.join(stopPoints, Seq("stop_point_id"))
  }

  def combineDataFrames(dfs: Seq[DataFrame]): DataFrame = {
    if (dfs.isEmpty) {
      throw new IllegalArgumentException("No DataFrames to combine")
    }

    dfs.reduce((df1, df2) => df1.union(df2))
  }
}
