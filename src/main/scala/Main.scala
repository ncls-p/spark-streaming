package org.example.spark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.SparkConf
import scalaj.http.Http
import org.json4s.DefaultFormats
import java.util.concurrent.{Executors, TimeUnit}
import java.nio.file.{Paths, Files, Path}
import java.nio.file.attribute.PosixFilePermissions
import java.nio.charset.StandardCharsets

object MainClass extends SparkSessionTrait {
  implicit val formats: DefaultFormats.type = DefaultFormats

  def main(args: Array[String]): Unit = {
    SparkSessionTrait("MyAppName", new SparkConf().setAppName("MyAppName").setMaster("local[*]"))

    SparkSessionTrait.turnOffLoggers()

    val scheduler = Executors.newScheduledThreadPool(1)

    val task = new Runnable {
      def run(): Unit = {
        // Download JSON data and save to file
        downloadJson("https://api.sncf.com/v1/coverage/sncf/lines?count=2000", "sncf_lines")
        downloadJson("https://api.sncf.com/v1/coverage/sncf/", "sncf_path")
        downloadJson("https://api.sncf.com/v1/coverage/sncf/lines/line:SNCF:A/stop_points", "sncf_stop_point_rer_a")
        downloadJson("https://api.sncf.com/v1/coverage/sncf/stop_areas/", "sncf_stop_areas")

        // Create Spark session
        val spark = SparkSession.builder().appName("JSON to DataFrame").getOrCreate()
        import spark.implicits._

        // Read JSON files into DataFrames
        val dfLines = readJson(spark, "./data/sncf_lines-*.json")
        val dfPath = readJson(spark, "./data/sncf_path-*.json")
        val dfStopPoints = readJson(spark, "./data/sncf_stop_point_rer_a-*.json")
        val dfStopAreas = readJson(spark, "./data/sncf_stop_areas-*.json")

        // Display DataFrames
        dfLines.show()
        dfPath.show()
        dfStopPoints.show()
        dfStopAreas.show()

        // Basic analysis on DataFrames
        println(s"Total lines: ${dfLines.count()}")
        println(s"Total stop points: ${dfStopPoints.count()}")
        println(s"Total stop areas: ${dfStopAreas.count()}")

        dfLines.printSchema()
        dfStopPoints.printSchema()
        dfStopAreas.printSchema()
      }
    }

    scheduler.scheduleAtFixedRate(task, 0, 1, TimeUnit.MINUTES)

    while (!Thread.currentThread.isInterrupted) {
      Thread.sleep(1000)
    }

    scheduler.shutdown()
    if (!scheduler.awaitTermination(1, TimeUnit.MINUTES)) {
      scheduler.shutdownNow()
    }
  }

  def downloadJson(url: String, filenamePrefix: String): Unit = {
    try {
      val response = Http(url)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .header("Authorization", "89c6d939-9245-494f-8b5a-1522fa890871")
        .asString
        .body

      val currentDate = java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm"))
      val jsonFilePath = s"./data/$filenamePrefix-$currentDate.json"

      val dirPath: Path = Paths.get("./data")
      if (!Files.exists(dirPath)) {
        Files.createDirectory(dirPath)
        Files.setPosixFilePermissions(dirPath, PosixFilePermissions.fromString("rwxrwxrwx"))
      }

      Files.write(Paths.get(jsonFilePath), response.getBytes(StandardCharsets.UTF_8))

      println(s"Data saved to: $jsonFilePath")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def readJson(spark: SparkSession, path: String): DataFrame = {
    spark.read.json(path)
  }
}
