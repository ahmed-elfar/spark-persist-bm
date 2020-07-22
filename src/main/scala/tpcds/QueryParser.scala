package tpcds

import java.nio.file.{Files, Paths}

import spark.TPCHOriginal.queriesCount

object QueryParser {


  val queriesPath = "/home/asherif/workspace/_tpcdsQnD/queries/query_0.sql"

  def getQueries(queriesPath: String): Array[String] = {

    val queryArr = Files.readAllLines(Paths.get(queriesPath)).toArray(new Array[String](50))
    println(queryArr.size)

    val queries = queryArr
      .map(line => {
        var tmp = line.trim
        if (tmp.startsWith("-- end")) {
          tmp = "#####"
        } else if (tmp.startsWith("-- start")) {
          tmp = ""
        }
        tmp
      }).filter(!_.startsWith(System.lineSeparator()))
      .filter(!_.isEmpty)
      .mkString(" ")
      .split("#####")
        .map(_.replaceAll("tpcds.", ""))
    println(queries.size)
    queries
  }
}
