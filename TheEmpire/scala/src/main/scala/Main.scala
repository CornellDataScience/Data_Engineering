package main.scala

import org.apache.spark.{SparkConf, SparkContext}

object Main extends App {

  val conf = new SparkConf().setAppName("kaggle").setMaster("local")
  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val dataframe_mysql = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/spark_data")
                        .option("driver", "com.mysql.jdbc.Driver").option("user", "spark").option("password", "spark")
                        .option("dbtable","train").load()

  println(dataframe_mysql.columns.mkString("Cols: [",", ","]"))
}
