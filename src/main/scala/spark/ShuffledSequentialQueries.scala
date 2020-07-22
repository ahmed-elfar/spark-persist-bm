package spark

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import TPCHOriginal._
import org.apache.spark.storage.StorageLevel

import scala.util.Random

object ShuffledSequentialQueries {


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

    for (a <- 1 to 4) {
      var queries = Random.shuffle(queriesOri)
      queries.foreach(tup2 => {
        print("Q" + tup2._2 + ", ")
        spark.time {
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

    if (!failedQueries.isEmpty) {
      println("Failed Queries : " + failedQueries.size)
      failedQueries.foreach(println(_))
    }

    Thread.sleep(10000000)
  }


}
