package org.example.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionTrait {

  def createSparkConf: SparkConf = new SparkConf()

  lazy val spark: SparkSession =
    try {
      SparkSession.builder().config(createSparkConf).getOrCreate()
    } catch {
      case e: Throwable =>
        throw new Exception("Failed to create a Spark Session: " + e.getMessage)
    }

  lazy val fileSystem: FileSystem = {
    try {
      val lConf: Configuration = spark.sparkContext.hadoopConfiguration
      org.apache.hadoop.fs.FileSystem.get(lConf)
    } catch {
      case e: Throwable =>
        throw new Exception("Failed to create a FileSystem: " + e.getMessage)
    }
  }

  object SparkSessionTrait {
    def apply(
        iAppName: String,
        iSparkConf: SparkConf = new SparkConf(),
        iMaster: String = "local[*]"
    ): Unit = {
      initLocalMode(iAppName, iMaster)

      println("APP Name :" + spark.sparkContext.appName)
      println("Deploy Mode :" + spark.sparkContext.deployMode)
      println("Master :" + spark.sparkContext.master)
    }

    def initLocalMode(iAppName: String, iMaster: String): Unit = {
      System.setProperty("hadoop.home.dir", "")
      new SparkSessionTrait {
        override def createSparkConf: SparkConf =
          new SparkConf().setAppName(iAppName).setMaster(iMaster)
      }.spark
    }

    def turnOffLoggers(): Unit = {
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
    }
  }
}
