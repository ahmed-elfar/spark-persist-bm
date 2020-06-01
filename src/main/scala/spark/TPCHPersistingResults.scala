package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import spark.TPCHOriginal._

object TPCHPersistingResults {

  def main(args: Array[String]): Unit = {

    val persistLevel = args(0) match {
      case "memory" => StorageLevel.MEMORY_ONLY
      case _ => StorageLevel.DISK_ONLY
    }

    val queries = load(queriesInputFile);

    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    registerTable(parquetDir, spark)

    val indexedQueries = queries.zip(Stream from 1)

    indexedQueries.foreach(tup2 => {
      print("Q" + tup2._2 + ", ")
      spark.time {
        spark.sql(tup2._1).persist(persistLevel).foreach(_ => ())
      }
    }
    )

    Thread.sleep(20000)

    indexedQueries.foreach(tup2 => {
      print("Q" + tup2._2 + ", ")
      spark.time {
        spark.sql(tup2._1).foreach(_ => ())
      }
    }
    )

  }
}
