package com.optaim.tool

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.mllib.feature.{HashingTF => MllibHashingTF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf

import scala.collection.mutable

/**
  * 求TDIDF的工具类
  * @param x
  * @param n
  */

class TFIDF(x:DataFrame,n:Int) extends Serializable {


  var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
  var wordsData = tokenizer.transform(x)

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
 // rescaledData.select($"category", $"words", $"features").show(false)




  val takeTopN = udf { (v: Vector) =>
    (v.toSparse.indices zip v.toSparse.values)
      .sortBy(-_._2) //负值就是从大到小
      .take(n)
      .map(x => mapWords.getOrElse(x._1, "null") + ":" + f"${x._2}%.3f".toString) // 冒号分隔单词和值,值取小数后三位
      .mkString(";") } // 词语和值的对以;隔开(别用逗号,会与hive表格式的TERMINATED BY ','冲突)

  //rescaledData.select(col("text"), takeTopN(col("features")).as("features")).show(false)
    rescaledData


}
