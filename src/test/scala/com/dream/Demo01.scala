package com.dream

import org.apache.spark.{SparkConf, SparkContext}

object Demo01 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("s").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //创建sparksql对象
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._


    //读取数据，对原始数据进行切分
    val datas = sc.textFile("D://originaldatas1.txt")
    val srcDF = datas.map { x =>
      val key = x.split("\t")(3)
      val va = (x.split("\t")(x.split("\t").length - 1))

      (key, va)

    }.filter(x=>x._2!="None").reduceByKey((x, y) => x+y).coalesce(1).saveAsTextFile("D://aaaaa")

  }
}
