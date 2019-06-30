package com.dream

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object TestProperty {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MySQL-Demo").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //通过并行化创建RDD
    val personRDD = sc.parallelize(Array("14 tom 5", "15 jerry 3", "16 kitty 6")).map(_.split(" "))
    //通过StrutType直接指定每个字段的schema
    val schema = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    )
    //将RDD映射到rowRDD
    val rowRDD = personRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).toInt))
    //将schema信息应用到rowRDD上
    val personDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    val properties = new Properties()
    val path = Thread.currentThread().getContextClassLoader.getResource("a.properties").getPath //文件要放到resource文件夹下
    properties.load(new FileInputStream(path))
//    println(properties.getProperty("ddd")) //读取键为ddd的数据的值
//    println(properties.getProperty("ddd", "没有值")) //如果ddd不存在,则返回第二个参数
//    properties.setProperty("ddd", "123") //添加或修改属性值

    val className=properties.getProperty("className")
    val url=properties.getProperty("url")
    val table=properties.getProperty("table")



//    val prop = new Properties()
//    prop.put("user", "root")
//    prop.put("password", "root")

    Class.forName(className).newInstance()

    personDataFrame.write.mode("overwrite").jdbc(url,
      table,properties)
    //停止SparkContext
    sc.stop()


  }

}
