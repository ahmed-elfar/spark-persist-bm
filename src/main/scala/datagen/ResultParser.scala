package datagen

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.regex.Pattern

import sys.Constants
import tpch.QueryParser.queriesPath

object ResultParser {

  val resultFile = Constants.HOME_PATH + "/tpch1g/result88.txt"

  def main(args: Array[String]): Unit = {

    val result = Files.readAllLines(Paths.get(resultFile)).toArray(new Array[String](10))
    val sortedResult = result.map(line => {
        val p = Pattern.compile("\\d+")
        val m = p.matcher(line)
        m.find()
        val queryNumber = Integer.valueOf(m.group())
        m.find()
        val time = Integer.valueOf(m.group())
        //println(queryNumber)
        (line, queryNumber, time)
      })
      //.sortBy(_._2)
      .map(tup3 => (tup3._2, tup3._3))
      .groupBy(tup => tup._1)
      .map(k => (k._1, k._2.map(_._2).reduce(_+_)))
      .map(k => (k._1, k._2 / 1000.0 / 60.0 /*/ 4*/))
      .map(value => (value._1, Math.round(value._2 * 100) / 100.0))
      .toList
      .sortBy(_._1)


    sortedResult.foreach(println(_))
  }
}
