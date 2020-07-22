package datagen

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object TestSelectSingleColumn {

  def main(args: Array[String]): Unit = {

    val persistBaseTbale = args(0) match {
      case "true" => true
      case _ => false
    }

    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read.parquet(GenerateData.wideParquetOut)
    println(df.count())

    if(persistBaseTbale){
      spark.time(df.persist(StorageLevel.DISK_ONLY).foreach(_ => ()))
      Thread.sleep(10000)
    }

    df.createOrReplaceTempView("line_item_wide")

    spark.time {
      //AIR
      spark.sql("select l_shipmode from line_item_wide").foreach(_ => ())
    }
    System.in.read()
  }

}
