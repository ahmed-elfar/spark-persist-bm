package persistquery

import org.apache.spark.sql.SparkSession
import spark.TPCHOriginal
import spark.TPCHOriginal.{parquetDir, registerTable}

object TestPersistQuery {


  val qFilter = "SELECT l_suppkey AS supplier_no,\n       sum(l_extendedprice * (1 - l_discount)) AS total_revenue\nFROM lineitem\nWHERE l_shipdate >= '1996-01-01'\n  AND l_shipdate < '1996-04-01'\nGROUP BY l_suppkey"
  val qAdditionalFilter = "SELECT l_suppkey AS supplier_no,\n       sum(l_extendedprice * (1 - l_discount)) AS total_revenue\nFROM lineitem\nWHERE l_shipdate >= '1996-01-01'\nGROUP BY l_suppkey"

  val qOriginal =  "SELECT l_suppkey\nFROM lineitem\nWHERE l_shipdate >= to_date('1992-04-17')"
  val qSelectFromOriginal = "SELECT l_suppkey\nFROM lineitem\nWHERE l_shipdate >= to_date('1992-04-17')\n  AND l_shipdate < to_date('1992-07-16')"

  val q1 = "SELECT s_suppkey,\n       s_name,\n       s_address,\n       s_phone,\n       l_extendedprice\nFROM supplier,\n     lineitem\nWHERE s_suppkey = l_suppkey\nORDER BY s_suppkey"
  val q2 = "SELECT s_suppkey,\n       s_name,\n       s_address,\n       s_phone\nFROM supplier,\n     lineitem\nWHERE s_suppkey = l_suppkey\nORDER BY s_suppkey"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    registerTable(parquetDir, spark)

    //testSelect(spark, q1, q2)
    //testSelect(spark, qOriginal, qSelectFromOriginal)
    testSelect2(spark, q1, q2)
    //spark.sql("select Distinct(l_shipdate) from lineitem order by l_shipdate").show(200, false)
    //spark.sql("select Distinct(l_shipdate) from lineitem order by l_shipdate").printSchema()
    Thread.sleep(100000000)

  }

  private def testSelect(spark: SparkSession, q1: String, q2:String) = {
    val df1 = spark.sql(q1).persist()
    spark.time(df1.foreach(_ => ()))
    println(df1.count())
    df1.printSchema()
    df1.explain(true)

    val df2 = spark.sql(q2).persist()
    spark.time(df2.foreach(_ => ()))
    println(df2.count())
    df2.printSchema()
    df2.explain(true)
  }

  private def testSelect2(spark: SparkSession, q1: String, q2:String) = {
    val df1 = spark.sql(q1).persist()
    spark.time(df1.foreach(_ => ()))
    println(df1.count())
    df1.printSchema()
    df1.explain(true)

    val df2 = df1.select("s_suppkey", "s_name", "s_address", "s_phone").persist()
    spark.time(df2.foreach(_ => ()))
    println(df2.count())
    df2.printSchema()
    df2.explain(true)
  }
}
