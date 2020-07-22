package spark

import java.io.File

import org.apache.spark.sql.{SaveMode, SparkSession}
import spark.TPCHOriginal.{load, parquetDir, queriesInputFile}
import spark.TPCHPersistBaseTableOrderedBykey.keyColumns

object HiveTPCH {

  def registerTable(path: String, spark: SparkSession): Unit = {
    val rootParquetDire = new File(path)
    rootParquetDire.listFiles().filter(_.isDirectory).map(dir => (dir.getName, dir.getAbsolutePath))
      .foreach(tup => {
        spark.sql(s"DROP TABLE IF EXISTS ${tup._1}")
        keyColumns.foreach(key => {
          if (key._1.equals(tup._1)) {
            val df = spark.read.parquet(tup._2)
            val stringCol = key._2.map(col => col.toString())
            df.write.mode(SaveMode.Overwrite)
              //.bucketBy(24, stringCol.head, stringCol.tail:_*)
              //.sortBy(stringCol.head, stringCol.tail:_*)
              .saveAsTable(tup._1)
          }
        })

        spark.sql(s"SHOW CREATE TABLE ${tup._1}").show(100,false)
      })
  }

  def main(args: Array[String]): Unit = {
    val queries = load(queriesInputFile);

    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    //registerTable(parquetDir, spark)

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
