package spark

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import spark.TPCHOriginal.{load, parquetDir, queriesInputFile}

object MultipleContext {


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

    val spark1 = SparkSession.builder().getOrCreate()
    spark1.sparkContext.setLogLevel("WARN")

    val storageLevel = args(0) match {
      case "memory" => StorageLevel.MEMORY_ONLY
      case "offHeap" => StorageLevel.OFF_HEAP
      case _ => StorageLevel.DISK_ONLY
    }

    spark1.time(registerTable2(parquetDir, spark1, storageLevel))
    Thread.sleep(5000)

    val sparkConf2 = new SparkConf().setAll(spark1.conf.getAll).set("spark.dynamicAllocation.maxExecutors", "12")
    //val sparContext = SparkContext.getOrCreate(sparkConf2)

    val spark2 = SparkSession.builder().config(sparkConf2).getOrCreate()
    //SparkSession.builder().config("spark.dynamicAllocation.maxExecutors", "12").getOrCreate()
    spark2.sparkContext.setLogLevel("WARN")

    queries.zip(Stream from 1).foreach(tup2 => {
      print("Q" + tup2._2 + ", ")
      spark2.time {
        spark2.sql(tup2._1).foreach(_ => ())
      }
    }
    )


    Thread.sleep(10000000)
  }
}
