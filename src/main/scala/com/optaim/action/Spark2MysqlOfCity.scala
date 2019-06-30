package com.optaim.action

import java.io.FileInputStream
import java.util.Properties

import com.optaim.co.AbstractParams
import org.apache.log4j.Logger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

/**
  * spark和数据库的交互
  */

object Spark2MysqlOfCity {

  private[this] val logger = Logger.getLogger(getClass().getName())

  case class Params(
                     input: String = null
                   ) extends AbstractParams[Params]


  def main(args: Array[String]) {
    val defParams = Params()

    val parser = new OptionParser[Params]("Spark2MysqlOfCity") {
      head("spark2MysqlOfCity")
      opt[String]("input")
        .required()
        .text("input raw log")
        .action((x, c) => c.copy(input = x))
    }

    parser.parse(args, defParams) match {
      case Some(x) => run(x)
      case _ => sys.exit(1)
    }
  }


  def run(params: Params): Unit = {
    logger.warn(s"Spark2MysqlOfCity with $params")
    val sc = new SparkContext(new SparkConf().setAppName(s"Spark2MysqlOfCity with $params"))
    val sqlContext = new SQLContext(sc)
    //通过并行化创建RDD
    val personRDD = sc.textFile(params.input).map(x=>x.substring(1,x.length-1)).map(_.split(","))
    //通过StrutType直接指定每个字段的schema
    val schema = StructType(
      List(
        StructField("city",StringType,true),
        StructField("keywords",StringType,true),
        StructField("weight",DoubleType,true)
      )
    )
    //将RDD映射到rowRDD
    val rowRDD = personRDD.map(p => Row(p(0).trim, p(1).trim, p(2).toDouble))
    //将schema信息应用到rowRDD上
    val personDataFrame = sqlContext.createDataFrame(rowRDD,schema)
    //创建Properties存储数据库相关属性
    val properties = new Properties()
    val path = Thread.currentThread().getContextClassLoader.getResource("a.properties").getPath //文件要放到resource文件夹下
    properties.load(new FileInputStream(path))
    val className=properties.getProperty("className")
    val url=properties.getProperty("url")
    val table=properties.getProperty("tableCity")
    Class.forName(className).newInstance()
    personDataFrame.write.mode("overwrite").jdbc(url,
      table,properties)
    sc.stop()
  }
}
