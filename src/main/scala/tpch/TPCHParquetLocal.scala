package tpch

import org.apache.spark.sql.{SaveMode, SparkSession}
import spark.TPCHOriginal.{load, registerTable2}

import scala.collection.mutable.ListBuffer

object TPCHParquetLocal {

  val queriesInputFile = "/tpch1g/tpch_queries_1tb.sql"
  val parquetDir = "/tpch1g/tpch/30/parquet"
  val queriesCount = 22

  def main(args: Array[String]): Unit = {

    val queries = load(queriesInputFile);

    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    registerTable2(parquetDir, spark)

    val failedQueries = new ListBuffer[Int]

    queries.zip(Stream from 1).foreach(tup2 => {
      if (tup2._2.equals(1)) {
        print("Q" + tup2._2 + ", ")
        spark.time {
          try {
            spark.sql(tup2._1).write.mode(SaveMode.Overwrite).parquet("/tpch1g/test/q1")
            spark.sql("select * from lineitem").repartition().write.mode(SaveMode.Overwrite).parquet("/tpch1g/test/lineitem")
          } catch {
            case e: Exception => {
              failedQueries += tup2._2
              e.printStackTrace()
            }
          }
        }
      }
    })

    if (!failedQueries.isEmpty) {
      println("Failed Queries : " + failedQueries.size)
      failedQueries.foreach(println(_))
    }

    Thread.sleep(10000000)
  }

}
