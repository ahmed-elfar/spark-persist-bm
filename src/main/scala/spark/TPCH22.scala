package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import spark.TPCHOriginal.{load, parquetDir, queriesInputFile}

import scala.collection.mutable.ListBuffer

object TPCH22 {


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

    spark.time {
      queriesOri.map(tup2 => {
        val runnable = new Runnable {
          override def run(): Unit = {
            val str = "Q" + tup2._2 + ", "
            val startTime = System.currentTimeMillis()
            try {
              spark.sql(tup2._1).foreach(_ => ())
            } catch {
              case e: Exception => {
                failedQueries += tup2._2
                e.printStackTrace()
              }
            }
            val endTime = System.currentTimeMillis()
            println(str + (endTime - startTime) + " ms")
          }
        }
        runnable
      }).map(new Thread(_))
        .map(thread => {
          thread.start()
          thread
        }
        ).foreach(_.join())
    }

    Thread.sleep(10000000)
  }
}
