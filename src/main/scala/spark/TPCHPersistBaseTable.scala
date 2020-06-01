package spark

import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import spark.TPCHOriginal._

object TPCHPersistBaseTable {

  def registerTable(path: String, spark: SparkSession): Unit = {
    val rootParquetDire = new File(path)
    rootParquetDire.listFiles().filter(_.isDirectory).map(dir => (dir.getName, dir.getAbsolutePath))
      .foreach(tup => {
        val df = spark.read.parquet(tup._2).persist(StorageLevel.DISK_ONLY)
        df.foreach(_ => ())
        df.createOrReplaceTempView(tup._1)
      })
  }

  def main(args: Array[String]): Unit = {

    val queries = load(queriesInputFile);

    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    spark.time(registerTable(parquetDir, spark))
    Thread.sleep(20000)

    queries.zip(Stream from 1).foreach(tup2 => {
      print("Q" + tup2._2 + ", ")
      spark.time {
        spark.sql(tup2._1).foreach(_ => ())
      }
    }
    )

  }
}
