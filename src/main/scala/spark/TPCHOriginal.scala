package spark

import java.io.File
import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import org.apache.spark.sql.{SaveMode, SparkSession}
import sun.misc.ClassLoaderUtil
import sys.Constants

import scala.collection.mutable.ListBuffer

object TPCHOriginal {

  val queriesInputFile =  Constants.HOME_PATH + "/SparkPersistBenchMark/tpch_sql_queries_raw.txt"
  val parquetDir = Constants.HOME_PATH + "/tpch1g/tpch/10/parquet"
  val queriesCount = 22

  val parquetFiles = List(
    "gs://ic-5sqv-bucket/tpch-1tb/compacted/tpch/customer.1594838009611",
    "gs://ic-5sqv-bucket/tpch-1tb/compacted/tpch/lineitem.1594842783607",
    "gs://ic-5sqv-bucket/tpch-1tb/compacted/tpch/nation.1594836418990",
    "gs://ic-5sqv-bucket/tpch-1tb/compacted/tpch/orders.1594837792973",
    "gs://ic-5sqv-bucket/tpch-1tb/compacted/tpch/part.1594838017039",
    "gs://ic-5sqv-bucket/tpch-1tb/compacted/tpch/partsupp.1594843178940",
    "gs://ic-5sqv-bucket/tpch-1tb/compacted/tpch/region.1594836418915",
    "gs://ic-5sqv-bucket/tpch-1tb/compacted/tpch/supplier.1594836524937"
  )

  def load(path: String): Array[String] = {
    Files.readAllLines(Paths.get(path)).toArray(new Array[String](queriesCount))
    //    Files.readAllLines(Paths.get(getClass.getClassLoader.getResource("tpch_sql_queries_raw.txt").toURI), StandardCharsets.UTF_8)
    //      .toArray(new Array[String](queriesCount))
  }

  def registerTable(path: String, spark: SparkSession): Unit = {
    parquetFiles
      .map(dir => {
        val arr = dir.split("/")
        (arr(arr.length - 1).split("\\.")(0), dir)
      })
      .foreach(tup => {
        val df = spark.read.parquet(tup._2)
        df.createOrReplaceTempView(tup._1)
      })
  }

  def registerTable2(path: String, spark: SparkSession): Unit = {
    val rootParquetDire = new File(path)
    rootParquetDire
      .listFiles()
      .filter(_.isDirectory)
      .map(dir => (dir.getName, dir.getAbsolutePath))
      .foreach(tup => {
        val df = spark.read.parquet(tup._2)
        df.createOrReplaceTempView(tup._1)
      })
  }

  def main(args: Array[String]): Unit = {
    val queries = load(queriesInputFile);

    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    registerTable2(parquetDir, spark)

    //warmUp(spark)

    //    queries.zip(Stream from 1).foreach(tup2 => {
    //      //if(tup2._2==12 || tup2._2==16){
    //        print("Q" + tup2._2 + ", ")
    //        spark.time {
    //          spark.sql(tup2._1).foreach(_ => ())
    //        }
    //      //}
    //    }
    //    )

    val failedQueries = new ListBuffer[Int]
    spark.time {
      queries.zip(Stream from 1).foreach(tup2 => {
        //if (!(tup2._2 == 12 || tup2._2 == 16)) {
          print("Q" + tup2._2 + ", ")
          spark.time {
            try {
              //if(tup2._2 == 18)
              //spark.sql(tup2._1).write.mode(SaveMode.Overwrite).parquet("/home/asherif/spark-warehouse/" + tup2._2)
              spark.sql(tup2._1).foreach(_ => ())
            } catch {
              case e: Exception => {
                failedQueries += tup2._2
                //e.printStackTrace()
              }
            }
          }
        //}
      })
    }

    if (!failedQueries.isEmpty) {
      println("Failed Queries : " + failedQueries.size)
      failedQueries.foreach(println(_))
    }

    Thread.sleep(10000000)
  }

  private def warmUp(spark: SparkSession) = {
    spark.sql("select * from lineitem limit 10000").foreach(_ => ())
  }

}
