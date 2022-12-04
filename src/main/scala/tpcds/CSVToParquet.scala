package tpcds

import java.io.File

import org.apache.spark.sql.{SaveMode, SparkSession}

object CSVToParquet {


  def main(args: Array[String]): Unit = {

    val headers = "/tpcdsGen/data.header"
    val csvs = "/_tpcdsQnD/1/tpl"
    val parquetOut = "/_tpcdsQnD/1/parquet"

    val spark = SparkSession.builder().master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val headersDir = new File(headers)
    val rootCSVDire = new File(csvs)

    headersDir.listFiles().filter(_.isFile).map(dir => (dir.getName.split("\\.")(0), dir.getAbsolutePath))
      .foreach(nameHeader => {
        rootCSVDire.listFiles().filter(_.isFile).map(dir => (dir.getName.split("\\.")(0), dir.getAbsolutePath))
          .foreach(nameCSV => {
          if (nameCSV._1.equals(nameHeader._1)) {
            println(nameCSV._1)
            println(nameHeader._1)
            val dfHeader = spark.read
              .format("csv")
              .option("header", "true")
              //.option("inferSchema", "true")
              .option("delimiter", "|")
              .load(nameHeader._2)

            val dfCSV_ = spark.read
              .format("csv")
              .option("inferSchema", "true")
              .option("delimiter", "|")
              //.schema(dfHeader.schema)
              .load(nameCSV._2)

            val dfCSV = dfCSV_.drop(dfCSV_.schema.last.name)

            if(dfCSV.columns.size != dfHeader.columns.size){
              throw new Exception("Columns Size are not  matching : " + nameCSV._1);
            }

            val headerColumnNames = dfHeader.columns
            val csvColumnNames = dfCSV.columns

            var df = dfCSV
            headerColumnNames.zipWithIndex.foreach(col => {
              df = df.withColumnRenamed(csvColumnNames(col._2), col._1)
            })

            println("================================================")
            df.printSchema()
            df.show(2, false)
            println("================================================")

            df.write.mode(SaveMode.Overwrite)
              .parquet(parquetOut + "/" + nameCSV._1)
          }
        })

      })

  }


}
