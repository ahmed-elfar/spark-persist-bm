package datagen

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object TestSelectAllColumn {

  def main(args: Array[String]): Unit = {

    val persistBaseTbale = args(0) match {
      case "true" => true
      case _ => false
    }

    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read.parquet(GenerateData.tallTable)

    if(persistBaseTbale){
      spark.time(df.persist(StorageLevel.DISK_ONLY).foreach(_ => ()))
      Thread.sleep(10000)
    }

    df.createOrReplaceTempView("line_item_expanded")

    spark.time {
      spark.sql("select * from line_item_expanded").foreach(_ => ())
    }
  }

}
