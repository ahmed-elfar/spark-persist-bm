package tpch

import java.io.File
import java.nio.file.{Files, Paths}

import spark.TPCHOriginal.queriesCount
import sys.Constants

object SparkBenchInput {


  def main(args: Array[String]): Unit = {

    val template = "{\n name = \"sql\",\n queryid =\"Q%s\",\n query = \"%s\"\n}\n"


    val str = Files.readAllLines(Paths.get(Constants.HOME_PATH + "/tpch1g/tpch_queries_1tb.sql")).toArray(new Array[String](queriesCount))
    .zip(Stream from 1)
    .map(query => {
      String.format(template, String.valueOf(query._2), query._1)
    }).mkString(",\n")

    println(str)
    //generateInputPath
  }

  private def generateInputPath = {
    val rootParquetDire = new File(Constants.HOME_PATH + "/tpch1g/tpch/1/parquet")
    val str = rootParquetDire
      .listFiles()
      .filter(_.isDirectory)
      .map(dir => (dir.getName, dir.getAbsolutePath))
      .map(x => x._1 + ":" + "file://" + x._2).mkString(",")

    println(str)
  }
}
