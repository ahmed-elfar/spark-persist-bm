package spark

import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import spark.TPCHOriginal._
import org.apache.spark.sql.functions.col

object TPCHPersistBaseTableOrderedBykey {


  val keyColumns = List(
    ("lineitem", List(col("l_orderkey"), col("l_linenumber"))),
    ("orders", List(col("o_orderkey"))),
    ("partsupp", List(col("ps_partkey"), col("ps_suppkey"))),
    ("part", List(col("p_partkey"))),
    ("supplier", List(col("s_suppkey"))),
    ("customer", List(col("c_custkey"))),
    ("nation", List(col("n_nationkey"))),
    ("region", List(col("r_regionkey")))
  )

  def registerTable(path: String, spark: SparkSession): Unit = {
    val rootParquetDire = new File(path)
    rootParquetDire.listFiles().filter(_.isDirectory).map(dir => (dir.getName, dir.getAbsolutePath))
      .foreach(tup => {
        keyColumns.foreach(key => {
          if (key._1.equals(tup._1)) {
            val df = spark.read.parquet(tup._2)/*.repartition(key._2: _*)*/.orderBy(key._2: _*).persist(StorageLevel.OFF_HEAP)
            df.foreach(_ => ())
            df.createOrReplaceTempView(tup._1)
          }
        })
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

    Thread.sleep(10000000)

  }
}
