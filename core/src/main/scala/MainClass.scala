package org.example.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.example.spark.downloader.JsonDownloader
import org.example.spark.processor.DataProcessor
import org.example.spark.scheduler.TaskScheduler
import java.nio.file.{Paths, Files}
import java.util.concurrent.TimeUnit

object MainClass extends SparkSessionTrait {
  def main(args: Array[String]): Unit = {
    implicit val formats = org.json4s.DefaultFormats

    try {
      val conf = new SparkConf().setAppName("MyAppName").setMaster("local[*]")
      val spark = SparkSession.builder.config(conf).getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")

      // List of RER lines to process
      val rerLines = List("A", "B", "C", "D", "E")
      val baseUrl = "https://api.sncf.com/v1/coverage/sncf/lines/line:SNCF:"
      val endpoints = List(
        "" -> "sncf_lines_rer",
        "/stop_points" -> "sncf_stop_point_rer",
        "/stop_areas" -> "sncf_stop_areas_rer",
        "/vehicle_journeys" -> "sncf_vehicle_journeys_rer"
      )

      val task = new Runnable {
        def run(): Unit = {
          try {
            val allTrainSchedules = rerLines.flatMap { line =>
              endpoints.foreach { case (endpoint, prefix) =>
                val url = s"$baseUrl$line$endpoint?count=2000"
                val filePrefix = s"${prefix}_$line"
                if (!JsonDownloader.downloadJson(url, filePrefix)) return
              }

              val paths = Map(
                s"./data/sncf_lines_rer_$line-*.json" -> "dfLines",
                s"./data/sncf_stop_point_rer_$line-*.json" -> "dfStopPoints",
                s"./data/sncf_stop_areas_rer_$line-*.json" -> "dfStopAreas",
                s"./data/sncf_vehicle_journeys_rer_$line-*.json" -> "dfVehicleJourneys"
              )

              val dfs = paths.map { case (path, name) =>
                name -> DataProcessor.readJson(spark, path)
              }

              val trainSchedulesWithNames = DataProcessor.processTrainSchedules(
                dfs("dfVehicleJourneys"),
                dfs("dfStopPoints")
              )
              trainSchedulesWithNames.show(100, truncate = false)

              Some(trainSchedulesWithNames.withColumn("rer_line", lit(line)))
            }

            var combinedTrainSchedules = spark.createDataFrame(
              spark.sparkContext.emptyRDD[Row],
              allTrainSchedules.head.schema
            )

            allTrainSchedules.foreach { df =>
              combinedTrainSchedules = combinedTrainSchedules.union(df)
            }

            val combinedCsvPath =
              "../socket-server/csv/combined_train_schedules_rer"
            val combinedFile = Paths.get(combinedCsvPath)
            val combinedSaveMode =
              if (Files.exists(combinedFile)) SaveMode.Overwrite
              else SaveMode.Overwrite

            println(
              s"Attempting to save combined train schedules CSV for all RER lines to: $combinedCsvPath"
            )
            combinedTrainSchedules
              .coalesce(1)
              .write
              .mode(combinedSaveMode)
              .option("header", true)
              .csv(combinedCsvPath)
            println(
              s"Combined train schedules for all RER lines saved to: $combinedCsvPath"
            )
          } catch {
            case e: Exception =>
              e.printStackTrace()
              System.err.println(s"Failed to process task: ${e.getMessage}")
          }
        }
      }

      TaskScheduler.scheduleTask(task, 0, 1, TimeUnit.MINUTES)
      while (!Thread.currentThread.isInterrupted) {
        Thread.sleep(1000)
      }
    } catch {
      case e: Throwable =>
        System.err.println(
          "Exception occurred during Spark Session initialization: " + e.getMessage
        )
        e.printStackTrace()
        throw e
    }
  }
}
