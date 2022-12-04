package tpch

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.regex.{Matcher, Pattern}

object QueryParser {

  val queriesPath = "/tpch1g/queries1tb"

  def main(args: Array[String]): Unit = {

    val rootParquetDire = new File(queriesPath)
    val queries = rootParquetDire
      .listFiles()
      .filter(!_.isDirectory)
      .map(file => {
        val p = Pattern.compile("\\d+")
        val m = p.matcher(file.getName)
        m.find()
        val queryNumber = Integer.valueOf(m.group())
        println(queryNumber)
        (file.getPath, queryNumber)
      })
      .sortBy(_._2)
      .map(_._1)
      .map(queryFile => {
        println(queryFile)
        queryFile
      }
      )
      .map(queryPath => {
        Files.readAllLines(Paths.get(queryPath)).toArray(new Array[String](10))
          .map(line => {
            println(line)
            line
          }
          )
          .filter(_ != null)
          .filter(!_.startsWith("--"))
          .filter(!_.startsWith("\n"))
          .filter(!_.isEmpty)
          .map(_.trim)
          .mkString(" ")
      })

    queries.foreach(println(_))

  }

}
