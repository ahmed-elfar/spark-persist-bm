package spark

import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import spark.TPCHOriginal.{load, parquetDir, queriesInputFile}

object CachingTablesUsingCatalog {

  def registerTable(path: String, spark: SparkSession, storageLevel: StorageLevel): Unit = {
    val rootParquetDire = new File(path)
    rootParquetDire.listFiles().filter(_.isDirectory).map(dir => (dir.getName, dir.getAbsolutePath))
      .foreach(tup => {
        val df = spark.read.parquet(tup._2)
        df.createOrReplaceTempView(tup._1)
        spark.catalog.cacheTable(tup._1)
        spark.sql(s"Select * from ${tup._1}").foreach(_=> ())
      })
  }

  def main(args: Array[String]): Unit = {

    val queries = load(queriesInputFile);

    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val storageLevel = args(0) match {
      case "memory" => StorageLevel.MEMORY_ONLY
      case _ => StorageLevel.DISK_ONLY
    }

    spark.time(registerTable(parquetDir, spark, storageLevel))
    Thread.sleep(20000)

    queries.zip(Stream from 1).foreach(tup2 => {
      print("Q" + tup2._2 + ", ")
      spark.time {
        spark.sql(tup2._1).foreach(_ => ())
      }
    }
    )

    Thread.sleep(10000000)
  }

}
