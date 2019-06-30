package com.dream

import com.optaim.util.{CASE, SDDS}
import org.ansj.splitWord.analysis.DicAnalysis
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.mllib.feature.{HashingTF => MllibHashingTF}

import scala.collection.mutable

object ParticipleAndTFIDF {
  val conf=new SparkConf().setAppName("s").setMaster("local[*]")
  val sc=new SparkContext(conf)

  //创建sparksql对象
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._


  //读取数据，对原始数据进行切分
  val datas=sc.textFile("D://originaldatas1.txt")
  val srcDF=datas.map{x=>
    val key=x.split("\t")(4)
    val va=(x.split("\t")(x.split("\t").length-1))
    val value=DicAnalysis.parse(va).recognition(SDDS.stopNatures).recognition(SDDS.stopWords()).toStringWithOutNature("|")
    //val value=va1.split("\\|")

    (key,value)
  }
    //.foreach(println)
    .filter(m=>(m._2)!="none").coalesce(1)

    //二次切分数据过滤掉太短的或含有数字的词

    .map { x =>
    var str=""
    val value1 = x._2.split("\\|")
    for(value2<-value1){
      if(value2.length>1&&value2.matches( "[\u4e00-\u9fa5]+")){
        if(str=="") {
          str=value2
        }
        else{ str = str + " "+ value2}
      }
    }
    (x._1,str)
  }.map{x =>



    //将数据转化为向量，利用TF-IDF算法统计每个分词的TFIDF值

    CASE.RawDataRecord(x._1,x._2)}.toDF()
  //    srcDF.select("category", "text").take(2).foreach(println)

  var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
  var wordsData = tokenizer.transform(srcDF)

  //    wordsData.select($"category", $"text", $"words")

  val mllibHashingTF = new MllibHashingTF(1000)
  val mapWords = wordsData.select("words").rdd.map(row => row.getAs[mutable.WrappedArray[String]](0))
    .flatMap(x => x).map(w => (mllibHashingTF.indexOf(w), w)).collect.toMap



  //将每个词转换成Int型，并计算其在文档中的词频（TF）
  var hashingTF =
    new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(1000)
  var featurizedData = hashingTF.transform(wordsData)


  //    featurizedData.select($"category", $"words", $"rawFeatures")

  //计算TF-IDF值
  var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
  var idfModel = idf.fit(featurizedData)
  var rescaledData = idfModel.transform(featurizedData)
  rescaledData.select($"category", $"words", $"features").show(false)

  val takeTopN = udf { (v: Vector) =>
    (v.toSparse.indices zip v.toSparse.values)
      .sortBy(-_._2) //负值就是从大到小
      .take(3)
      .map(x => mapWords.getOrElse(x._1, "null") + ":" + f"${x._2}%.3f".toString) // 冒号分隔单词和值,值取小数后三位
      .mkString(";") } // 词语和值的对以;隔开(别用逗号,会与hive表格式的TERMINATED BY ','冲突)

  rescaledData.select(col("text"), takeTopN(col("features")).as("features")).show(false)


}
