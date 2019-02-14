package com.structure.test


import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer


object HiveStatusLog {
  var logMsgList = new ListBuffer[String]()

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\Users\\ocuments\\Softwares\\winutil")
    val hdfs = FileSystem.get(new URI("hdfs://namenode:8020"), new Configuration())
    val sparkCnxt: SparkContext = new SparkContext(new SparkConf().setAppName("DataHarmonize").setMaster("local[*]"))
    val sparkSess: SparkSession = SparkSession.builder().config("spark.master", "local").config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict").getOrCreate()
    val queryFileDF = sparkCnxt.textFile("/user/Test/QueryFile.txt").collect().mkString


    /* val hdfs = FileSystem.get(new URI("hdfs://namenode:8020"), new Configuration())

     val sparkCnxt: SparkContext = new SparkContext(new SparkConf().setAppName("HiveQueryExecution"))
     val sparkSess: SparkSession = SparkSession.builder().config("spark.master", "yarn").config("hive.exec.dynamic.partition", "true")
       .config("hive.exec.dynamic.partition.mode", "nonstrict").getOrCreate()
     val queryFileDF = sparkCnxt.textFile("hdfs://namenode:8020/user/Test/QueryFile.txt").collect().mkString
   */  queryFileDF.split(";").foreach(query => executeQuery(query))

    /*if (logMsgList.size != 0) {
      MailProcessor.sendMailReport("Error in Hive Query Execution", logMsgList.mkString("\n"))
    }*/

    def executeQuery(queryline: String): Unit = {
      try {
        val query = queryline.split("=")(1)
        val table = queryline.split("=")(0)
        sparkSess.sql(query)       
        sparkSess.sql("Insert into CDRP_DB.status_log values ('" + table + "','Query Executed'," + "'Success')")
      } catch {
        /*case e: Exception => logMsgList += "Exception in Executing Hive query --->: " + queryline
          logMsgList += e.toString*/

        case e: Exception =>
          val query = queryline.split("=")(1)
          val table = queryline.split("=")(0)
          sparkSess.sql("Insert into CDRP_DB.status_log values ('" + table + "','" +queryline + "--" + e.toString + "'," + "'Failed')")
        /*
                  logMsgList += "Exception in Executing Hive query --->: " + queryline
                  logMsgList += e.toString*/

      }
    }
  }
}
