package com.optaim.action

import java.util

import com.optaim.co.AbstractParams
import com.optaim.tool.{TFIDF, TFIDFCos}
import com.optaim.util.{CASE, ParameterOfCos, ParticipleOfAnsj, ParticipleOfFilter}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

/**
  * 景点关键词计算的主类
  */

object WeightOfSight {

  private[this] val logger = Logger.getLogger(getClass().getName());

  case class Params(
                     numPartition: Int = 1,
                     input: String = null,
                     output1: String = null,
                     output2: String = null,
                     output3: String = null,
                     output4: String = null,
                     output5: String = null,
                     output: String = null
                   ) extends AbstractParams[Params]


  def main(args: Array[String]) {
    val defParams = Params()

    val parser = new OptionParser[Params]("WeightOfSight") {
      head("weightOfSight")
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
      opt[String]("output4")
        .required()
        .text("output path to save model and log")
        .action((x, c) => c.copy(output4 = x))
      opt[String]("output5")
        .required()
        .text("output path to save model and log")
        .action((x, c) => c.copy(output5 = x))
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
    logger.warn(s"WeightOfSight with $params")
    val sc = new SparkContext(new SparkConf().setAppName(s"WeightOfSight with $params"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val datas = sc.textFile(params.input)
    val srcDF = datas.map { x =>
      ParticipleOfAnsj.ansjMethod1(x)
    }.filter(m => (m._2) != "none").coalesce(1)
      //二次切分数据过滤掉太短的或含有数字的词
      .map { x =>ParticipleOfFilter.ansjFilter1(x)}.map { x =>
      CASE.RawDataRecord1(x._1,x._2,x._3,x._4)
    }.toDF()

    //利用dataframe查出词频前100输出
    val a = new TFIDF(srcDF,100)
    a.rescaledData.select(col("category1"),col("category2"),col("category"), a.takeTopN(col("features")).as("features")).rdd.map{
      x=>x
    }.saveAsTextFile(params.output1)

    //利用dataframe查出词频最高的词输出
    val b=new TFIDF(srcDF,1)
    b.rescaledData.select(col("category"), b.takeTopN(col("features")).as("features")).rdd.map{
      x=>x.toString()
    }.map{x=>
      x.substring(1,x.length-1).trim
        .split(",")
    }.map{x=>
      if(x(1).split(":").length>1) {
        if(x(1).split(":")(1).toDouble>1) {
          (x(0), x(1).split(":")(1))
        }else{(x(0),100)}
      }else{
        (x(0),100)
      }
    }.saveAsTextFile(params.output2)


    sc.textFile(params.output1).map(x=>x.substring(1,x.length-1)).map{x=>
      x.split(",")
    }.map{x=>
      val list=new util.ArrayList[(String,String,String,String,String)]()
      val v=x(3).split(";")
      for(v1<-v){
        if(v1.split(":").length>1) {
          list.add((x(2), x(0), x(1), v1.split(":")(0), v1.split(":")(1)))
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


    val lefttable=sc.textFile(params.output3).map{x=>
      x.split(",")
    }.map{x=>
      if(x.length>4) {
        CASE.SightData(x(0), x(1), x(2), x(3), x(4))
      }else{
        CASE.SightData("", "", "", "", "")
      }

    }.toDF()


    val righttable=sc.textFile(params.output2).map{x=>
      x.split(",")
    }.map{x=>
      CASE.MaxSightData(x(0),x(1))
    }.toDF()

    lefttable.join(righttable,"sight").select("sight","viewId","city","keyWord","weight","maxData")
//      .show(false)
      .rdd.map{x=>
      x.toString()
    }
//      .foreach(println)
      .map{x=>
      x.substring(2,x.length-2)
        .split(",")
    }.map{x=>
      (x(0),x(1),x(2),x(3),(x(4).substring(0,x(4).length-1).toDouble/x(5).toDouble*100).formatted("%.4f"))
    }.map{x=>
        (x._4, x._5, x._1, x._2, x._3)
    }
      //TODO
      .filter(m=>m._1!=null&&m._1!="").filter(_._2.toDouble<=100).coalesce(10)
      .saveAsTextFile(params.output4)

  //风景余弦
    val lefttable1=sc.textFile(params.output4).map{x=>x.substring(1,x.length-1)}.map{x=>
      x.split(",")
    }.map{x=>
      CASE.SightData1(x(0), x(1), x(2))
    }.toDF()

    //TODO
    //TODO
    val documents = datas.map { x =>
      ParticipleOfAnsj.ansjMethod(x)
    }
      //.foreach(println)
      .filter(m => (m._2) != "none")

      //二次切分数据过滤掉太短的或含有数字的词

      .map { x =>
      ParticipleOfFilter.ansjFilter(x).toString().replace(",", " ")
    }
      //      .foreach(println)
      .map { x =>
      ParameterOfCos.separate(x)
    }
    //

    //TODO
    TFIDFCos.CosCaculate(documents,sc).filter(_._3>0.15).coalesce(params.numPartition).saveAsTextFile(params.output5)


    val righttable1=sc.textFile(params.output5).map{x=>x.substring(1,x.length-1)}.map{x=>
      x.split(",")
    }.map{x=>
      CASE.CosData(x(0),x(1),x(2))
    }.toDF()

    lefttable1.join(righttable1,"sight").select("sight","weight","keyWord","cosSight","cos").distinct()
      //    .show(false)
      .rdd.map{x=>
      x.toString()
    }.coalesce(params.numPartition).saveAsTextFile(params.output)

  }

}
