package tpcds

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object TPCDSParquet {

  val parquetDir = "/_tpcdsQnD/1/parquet"
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
    val queries = QueryParser.getQueries(QueryParser.queriesPath)

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    registerTable(parquetDir, spark)

    val failedQueries = new ListBuffer[Int]

    queries.zip(Stream from 1).foreach(tup2 => {
      //if(tup2._2==12 || tup2._2==16){
      print("Q" + tup2._2 + ", ")
      spark.time {
        try {
          spark.sql(tup2._1).foreach(_ => ())
        } catch {
          case e: Exception => {
            failedQueries+= tup2._2
            e.printStackTrace()
          }
        }
      }
      //}
    }
    )

    if(!failedQueries.isEmpty) {
      println("Failed Queries : " + failedQueries.size)
      failedQueries.foreach(println(_))
    }
    Thread.sleep(10000000)
  }


}
