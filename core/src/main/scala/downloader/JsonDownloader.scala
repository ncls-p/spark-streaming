package org.example.spark.downloader

import scalaj.http.Http
import java.nio.file.{Files, Paths, Path}
import java.nio.file.attribute.PosixFilePermissions
import java.nio.charset.StandardCharsets

object JsonDownloader {
  def downloadJson(url: String, filenamePrefix: String): Boolean = {
    try {
      val response = Http(url)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .header("Authorization", "0602d7a5-0027-41a7-b0c9-acdd0f6c5ee8")
        .asString

      if (response.code != 200) {
        println(s"Failed to fetch data from $url. Response code: ${response.code}. Response body: ${response.body}")
        return false
      }

      val body = response.body
      if (body.contains("Token absent") || body.contains("error")) {
        println(s"Error in response: $body")
        return false
      }

      val currentDate = java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm"))
      val jsonFilePath = s"./data/$filenamePrefix-$currentDate.json"

      val dirPath: Path = Paths.get("./data")
      if (!Files.exists(dirPath)) {
        Files.createDirectory(dirPath)
        Files.setPosixFilePermissions(dirPath, PosixFilePermissions.fromString("rwxrwxrwx"))
      }

      Files.write(Paths.get(jsonFilePath), body.getBytes(StandardCharsets.UTF_8))

      println(s"Data saved to: $jsonFilePath")
      true
    } catch {
      case e: Exception =>
        e.printStackTrace()
        false
    }
  }
}