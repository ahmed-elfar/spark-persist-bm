package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import spark.TPCHOriginal.{load, parquetDir, queriesInputFile}

import scala.collection.mutable.ListBuffer
import scala.util.Random

object ConcurrentQueries {

  def main(args: Array[String]): Unit = {
    val queriesOri = load(queriesInputFile).toList.zip(Stream from 1)

    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val storageLevel = args(0) match {
      case "memory" => StorageLevel.MEMORY_ONLY
      case "offHeap" => StorageLevel.OFF_HEAP
      case "disk" => StorageLevel.DISK_ONLY
      case _ => StorageLevel.NONE
    }

    TPCHPersistBaseTable.registerTable(parquetDir, spark, storageLevel)

    val failedQueries = new ListBuffer[Int]

    val runnable = new Runnable {
      override def run(): Unit = {
        var queries = Random.shuffle(queriesOri)
        queries.foreach(tup2 => {
          spark.time {
            print("Q" + tup2._2 + ", ")
            try {
              spark.sql(tup2._1).foreach(_ => ())
            } catch {
              case e: Exception => {
                failedQueries += tup2._2
                e.printStackTrace()
              }
            }
          }
        })
      }
    }

    val threadList = List.tabulate(4)(f => new Thread(runnable))

    threadList.map(thread => {
      thread.start()
      thread
    }).foreach(_.join())

    if (!failedQueries.isEmpty) {
      println("Failed Queries : " + failedQueries.size)
      failedQueries.foreach(println(_))
    }

    Thread.sleep(10000000)
  }

}
