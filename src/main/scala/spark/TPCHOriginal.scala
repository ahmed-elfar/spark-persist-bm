package spark

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.SparkSession

object TPCHOriginal {

  val queriesInputFile = "/home/asherif/IdeaProjects/SparkPersistBenchMark/tpch_sql_queries_raw.txt"
  val parquetDir = "/home/asherif/workspace/tpch1g/tpch/10/parquet"
  val queriesCount = 22

  def load(path: String): Array[String] = {
    Files.readAllLines(Paths.get(path)).toArray(new Array[String](queriesCount))
  }

  def registerTable(path: String, spark: SparkSession): Unit = {
    val rootParquetDire = new File(path)
    rootParquetDire.listFiles().filter(_.isDirectory).map(dir => (dir.getName, dir.getAbsolutePath))
      .foreach(tup => {
        val df = spark.read.parquet(tup._2)
        df.createOrReplaceTempView(tup._1)
      })
  }

  def main(args: Array[String]): Unit = {

    val queries = load(queriesInputFile);

    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    registerTable(parquetDir, spark)

    queries.zip(Stream from 1).foreach(tup2 => {
      print("Q" + tup2._2 + ", ")
      spark.time {
        spark.sql(tup2._1).foreach(_ => ())
      }
    }
    )

  }
}
