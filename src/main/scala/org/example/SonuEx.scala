package org.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object SonuEx extends App {
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","MavenDemo")
  sparkConf.set("spark.master","local[2]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val df1 = spark.read.format("csv").option("header","true").option("inferschema","true")
    .option("mode","DROPMALFORMED")
    .option("path","G:/Mydata/employee_record/employees.csv")
    .load()

    df1.printSchema()
    df1.show()
    spark.stop()

 }
