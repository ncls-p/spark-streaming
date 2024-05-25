package org.example.spark

import org.apache.spark.SparkConf

object MainClass extends SparkSessionTrait {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSessionTrait
    SparkSessionTrait("MyAppName", new SparkConf().setAppName("MyAppName").setMaster("local[*]"))

    // Turn off loggers
    SparkSessionTrait.turnOffLoggers()

    // Now you can use the `spark` and `fileSystem` instances
    val df = spark.read.json("./src/main/resources/people.json")
    df.show()

    val fsStatus = fileSystem.getStatus(new org.apache.hadoop.fs.Path("/"))
    println(fsStatus)
  }
}