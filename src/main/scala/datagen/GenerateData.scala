package datagen

import sys.Constants
import org.apache.spark.sql.{SaveMode, SparkSession, functions}

object GenerateData {

  val columns = 50
  val inputParquet =  Constants.HOME_PATH + "/tpch1g/tpch/10/parquet/lineitem"
  val wideParquetOut = Constants.HOME_PATH + "/tpch1g/tpch/10/lineItemWide"
  val columnToReplicate = "l_orderkey"
  val tallTable = Constants.HOME_PATH + "/tpch1g/tpch/10/lineItemTall"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    //val df = spark.read.parquet(wideParquetOut)
    //df.select("l_shipmode").groupBy("l_shipmode").agg(functions.count("l_shipmode").as("countDistinct")).show(1000, false)
    widenTable(spark)

    //expandTable(spark)

  }

  private def expandTable(spark: SparkSession) = {
    val lineItem_100 = spark.read.parquet(tallTable).persist()
    lineItem_100.count()
    val lineItem_100000 = (1 to 2).foldLeft(lineItem_100) {
      (df, columnNumber) => {
        val temp = df.unionByName(df).coalesce(32).persist()
        temp.foreach(_ => ())
        temp
      }
    }

    lineItem_100000.write.mode(SaveMode.Overwrite).parquet(tallTable)

    println(spark.read.parquet(tallTable).count())
  }

  private def widenTable(spark: SparkSession) = {
    val lineItem = spark.read.parquet(inputParquet).repartition(64)
    var lineItemWide = (1 to columns - lineItem.columns.length).foldLeft(lineItem) {
      (df, columnNumber) => df.withColumn(columnToReplicate + "_" + columnNumber, functions.col(columnToReplicate))
    }

    //lineItemWide = lineItemWide.unionByName(lineItemWide)

    println("Columns count after replication: " + lineItemWide.columns.length)
    lineItemWide.write.mode(SaveMode.Overwrite).parquet(wideParquetOut)
    lineItemWide.show(10, false)
  }
}
