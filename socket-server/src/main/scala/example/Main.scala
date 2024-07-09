package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.java_websocket.server.WebSocketServer
import org.java_websocket.WebSocket
import org.java_websocket.handshake.ClientHandshake
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.net.InetSocketAddress
import scala.collection.mutable
import org.apache.spark.SparkContext
import java.time.LocalDateTime

object SparkStreamingFileCSV {
  def main(args: Array[String]): Unit = {
    // Vérifier les arguments de ligne de commande

    // Initialiser Spark StreamingContext
    val conf = new SparkConf()
      .setAppName("Spark Streaming File CSV")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(10))

    // Initialise le serveur
    val server = new WebSocketServer(new InetSocketAddress("localhost", 8888)) {
      override def onOpen(conn: WebSocket, handshake: ClientHandshake): Unit = {
        println("Nouvelle connexion ouverte")
        connections += conn // Ajouter la nouvelle connexion à la liste
      }
      val connections = mutable.Set[WebSocket]()
      override def onClose(
          conn: WebSocket,
          code: Int,
          reason: String,
          remote: Boolean
      ): Unit = {
        println("Connexion fermée")
        connections -= conn // Supprimer la connexion fermée de la liste
      }

      override def onMessage(conn: WebSocket, message: String): Unit = {
        println("Message reçu: " + message)
        // Traitement ou réponse au message reçu si nécessaire
      }

      override def onError(conn: WebSocket, ex: Exception): Unit = {
        println("Erreur survenue: " + ex.getMessage)
      }

      override def onStart(): Unit = {
        println("Serveur démarré")
      }

      def sendToAll(message: String): Unit = {
        connections.foreach { conn =>
          conn.send(message)
        }
      }
    }
    server.start()

    // Récupérer le répertoire des arguments
    val directory = new java.io.File("./csv").getAbsolutePath
    // print current directory
    println("Current directory: " + new java.io.File(".").getCanonicalPath)
    println("csv directory: " + new java.io.File(directory).getCanonicalPath)
    println(
      "csv files: " + new java.io.File(directory).listFiles().mkString(", ")
    )

    // Ajoutez ces lignes après la définition de directory
    val dir = new java.io.File(directory)
    if (!dir.exists() || !dir.isDirectory) {
      println(
        s"Le répertoire $directory n'existe pas ou n'est pas un répertoire"
      )
      System.exit(1)
    }
    def listCsvFiles(dir: java.io.File): Array[java.io.File] = {
      val files = dir.listFiles()
      val csvFiles = files.filter(_.getName.endsWith(".csv"))
      val subDirCsvFiles = files.filter(_.isDirectory).flatMap(listCsvFiles)
      csvFiles ++ subDirCsvFiles
    }

    val csvFiles = listCsvFiles(dir)
    if (csvFiles.isEmpty) {
      println(s"Aucun fichier CSV trouvé dans $directory")
      System.exit(1)
    }
    println(s"Fichiers CSV trouvés : ${csvFiles.map(_.getName).mkString(", ")}")

    // Initialiser SparkSession pour lire des fichiers CSV
    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    // Lire les nouveaux fichiers CSV ajoutés au répertoire
    val csvStream = ssc.textFileStream(directory)
    println(s"csvStream created: $csvStream")

    csvStream.foreachRDD { rdd =>
      println(s"Processing new RDD at ${java.time.LocalDateTime.now()}")
      println(s"RDD partition count: ${rdd.getNumPartitions}")
      println(s"RDD is empty: ${rdd.isEmpty()}")

      if (!rdd.isEmpty()) {
        // Collect the data (be cautious with large datasets)
        val collectedData = rdd.collect()
        println(s"Collected data size: ${collectedData.length}")

        // Print each line of the CSV
        collectedData.take(5).foreach(line => println(s"CSV line: $line"))

        // Parse and send data 5 by 5
        collectedData.grouped(5).foreach { group =>
          val parsedGroup = group.map { line =>
            val fields = line.split(",")
            fields.mkString(", ")
          }
          val dataToSend = parsedGroup.mkString("\n")
          println(s"Sending data:\n$dataToSend")
          server.sendToAll(dataToSend)
          Thread.sleep(5000)
        }

      } else {
        println("RDD is empty, no new data to process")
      }
    }

    // Démarrer le contexte de streaming
    ssc.start()
    ssc.awaitTermination()
  }
}
