package com.test
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext


object HiveTable {

  def main(args: Array[String]): Unit = {
    //System.setProperty("HADOOP_USER_NAME","nso3prod")


    //val sparkConf = new SparkConf().setAppName("SparkHive").setMaster("local[*]")
    val sparkConf = new SparkConf().setAppName("SparkHive")
    val javaSparkCont = new JavaSparkContext(sparkConf)
    val sqlContext = new SQLContext(javaSparkCont.sc)


    javaSparkCont.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    javaSparkCont.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
    val datbase = sqlContext.sql("show tables in <DBNAME>")
    datbase.show()
    datbase.coalesce(1).write.option("header","true")
      .save("<HDFS PATH>")
  }
}


