package com.structure.test
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext


object HiveTable {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","nso3prod")


    //val sparkConf = new SparkConf().setAppName("SparkHive").setMaster("local[*]")
    val sparkConf = new SparkConf().setAppName("SparkHive")
    val javaSparkCont = new JavaSparkContext(sparkConf)
    val sqlContext = new SQLContext(javaSparkCont.sc)


    javaSparkCont.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    javaSparkCont.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")

    /*  val sparkSess: SparkSession = SparkSession.builder()
        .config("spark.sql.warehouse.dir","hdfs://dayrhegapd016.enterprisenet.org:8020/user/hive/warehouse/")
        .config("hive.metastore.uris","http://dayrhegapd016.enterprisenet.org:9083")
       // .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()*/

    val datbase = sqlContext.sql("show tables in crp_si_leaflet")
    datbase.show()
    datbase.coalesce(1).write.option("header","true")
      .save("hdfs://dayrhegapd016.enterprisenet.org:8020/user/nso3prod/Test/CategoryDB/")
  }
}


