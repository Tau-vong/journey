package com.optaim.action

import java.util

import com.optaim.co.AbstractParams
import com.optaim.tool.TFIDF
import com.optaim.util.{CASE, ParticipleOfAnsj, ParticipleOfFilter}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

/**
  * 城市关键词权重计算的主类
  */

object WeightOfKeywords {

  private[this] val logger = Logger.getLogger(getClass().getName());

  case class Params(
                     numPartition: Int = 1,
                     input: String = null,
                     output1: String = null,
                     output2: String = null,
                     output3: String = null,
                     output: String = null
                   ) extends AbstractParams[Params]


  def main(args: Array[String]) {
    val defParams = Params()

    val parser = new OptionParser[Params]("WeightOfKeywords") {
      head("weightOfKeywords")
      opt[Int]("numPartition")
        .text(s"number of partition files to save, default: ${defParams.numPartition}")
        .action((x, c) => c.copy(numPartition = x))
      opt[String]("input")
        .required()
        .text("input raw log")
        .action((x, c) => c.copy(input = x))
      opt[String]("output1")
        .required()
        .text("output path to save model and log")
        .action((x, c) => c.copy(output1 = x))
      opt[String]("output2")
        .required()
        .text("output path to save model and log")
        .action((x, c) => c.copy(output2 = x))
      opt[String]("output3")
        .required()
        .text("output path to save model and log")
        .action((x, c) => c.copy(output3 = x))
      opt[String]("output")
        .required()
        .text("output path to save model and log")
        .action((x, c) => c.copy(output = x))
    }

    parser.parse(args, defParams) match {
      case Some(x) => run(x)
      case _ => sys.exit(1)
    }
  }


  def run(params: Params): Unit = {
    logger.warn(s"WeightOfKeywords with $params")
    val sc = new SparkContext(new SparkConf().setAppName(s"WeightOfKeywords with $params"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val datas = sc.textFile(params.input)
    val srcDF = datas.map { x =>
      ParticipleOfAnsj.ansjMethod0(x)
    }.filter(x=>x._2!="None").reduceByKey((x, y) => x+y)
      .map { x =>
        val value=ParticipleOfAnsj.participle(x._2)
        (x._1, value)
      }
      .filter(m => (m._2) != "none")
      //TODO
      .coalesce(params.numPartition)
      //二次切分数据过滤掉太短的或含有数字的词
      .map { x =>
      ParticipleOfFilter.ansjFilter(x)
    }.map { x =>
      CASE.RawDataRecord(x._1, x._2)
    }.toDF()

    //利用dataframe计算城市分词前100
    val a=new TFIDF(srcDF,100)
    a.rescaledData.select(col("category"), a.takeTopN(col("features")).as("features")).rdd.map{
      x=>x
    }.saveAsTextFile(params.output1)

    //利用dataframe计算城市分词第一
    val b=new TFIDF(srcDF,1)
    b.rescaledData.select(col("category"), b.takeTopN(col("features")).as("features")).rdd.map{
      x=>x.toString()
    }.map{x=>
      x.substring(1,x.length-1).trim
        .split(",")
    }.map{x=>
      if(x(1).split(":").length>1) {
        (x(0), x(1).split(":")(1))
      }else{
        (x(0),100)
      }
    }.saveAsTextFile(params.output2)


    //将分词按城市拼成每个分词单行的
    sc.textFile(params.output1).map(x=>x.substring(1,x.length-1)).map{x=>
      x.split(",")
    }.map{x=>
      val list=new util.ArrayList[(String,String,String)]()
      val v=x(1).split(";")
      for(v1<-v){
        if(v1.split(":").length>1) {
          list.add((x(0), v1.split(":")(0), v1.split(":")(1)))
        }
      }
      list
    }.map{x=>
      var str=""
     for(i<- 0 to x.size()-1){
      str=str + x.get(i) + "\n"

     }
      str.trim
    }
      .saveAsTextFile(params.output3)


    //拿到城市每个分词的权重
    val lefttable=sc.textFile(params.output3).map{x=>
      x.split(",")
    }.map{x=>
        CASE.CityData(x(0),x(1),x(2))
    }.toDF()

    val righttable=sc.textFile(params.output2).map{x=>
      x.split(",")
    }.map{x=>
      CASE.MaxCityData(x(0),x(1))
    }.toDF()

    lefttable.join(righttable,"city").select("city","keyWord","weight","maxData").rdd.map{x=>
      x.toString()
    }
      .map{x=>
      x.substring(2,x.length-2)
        .split(",")
    }.map{x=>
      (x(0),x(1),(x(2).substring(0,x(2).length-1).toDouble/x(3).toDouble*100).formatted("%.4f"))
    }.coalesce(params.numPartition).saveAsTextFile(params.output)
//      .foreach(println)

  }

}
