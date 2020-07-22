package spark

import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import spark.TPCHOriginal._

object TPCHPersistBaseTable {

  def registerTable(path: String, spark: SparkSession, storageLevel: StorageLevel): Unit = {
    parquetFiles
      .map(dir => {
        val arr = dir.split("/")
        (arr(arr.length-1).split("\\.")(0), dir)
      })
      .foreach(tup => {
        val df = spark.read.parquet(tup._2)
          if(storageLevel != StorageLevel.NONE) df.persist(storageLevel)
        df.foreach(_ => ())
        df.createOrReplaceTempView(tup._1)
      })
  }

  def registerTable2(path: String, spark: SparkSession, storageLevel: StorageLevel): Unit = {
    val rootParquetDire = new File(path)
    rootParquetDire.listFiles().filter(_.isDirectory).map(dir => (dir.getName, dir.getAbsolutePath))
      .foreach(tup => {
        val df = spark.read.parquet(tup._2).persist(storageLevel)
        df.foreach(_ => ())
        df.createOrReplaceTempView(tup._1)
      })
  }

  def main(args: Array[String]): Unit = {

    val queries = load(queriesInputFile);

    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val storageLevel = args(0) match {
      case "memory" => StorageLevel.MEMORY_ONLY
      case "offHeap" => StorageLevel.OFF_HEAP
      case _ => StorageLevel.DISK_ONLY
    }

    spark.time(registerTable(parquetDir, spark, storageLevel))
    Thread.sleep(30000)

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
