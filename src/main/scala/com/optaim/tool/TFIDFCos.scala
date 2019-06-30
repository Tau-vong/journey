package com.optaim.tool

import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{SparseVector => SV}

/**
  * 求文本相似性余弦的工具类
  */

object TFIDFCos {



    def CosCaculate(documents:RDD[(String,Seq[String])],sc:SparkContext):RDD[(String,String,Double)]= {


      val hashingTF = new HashingTF(Math.pow(2, 18).toInt)
      //这里将每一行的行号作为doc id，每一行的分词结果生成tf词频向量
      val tf_num_pairs = documents.map {
        case (a, b) =>
          val tf = hashingTF.transform(b)
          (a, tf)
      }
      tf_num_pairs.cache()
      //构建idf model
      val idf = new IDF().fit(tf_num_pairs.values)
      //将tf向量转换成tf-idf向量
      val num_idf_pairs = tf_num_pairs.mapValues(v => idf.transform(v))
      //.foreach(println)
      //广播一份tf-idf向量集
      val b_num_idf_pairs = sc.broadcast(num_idf_pairs.collect())

      //计算doc之间余弦相似度
      val docSims = num_idf_pairs.flatMap {
        case (id1, idf1) =>
          val idfs = b_num_idf_pairs.value
//            .filter(_._1 != id1)
          val sv1 = idf1.asInstanceOf[SV]
          import breeze.linalg._
          val bsv1 = new SparseVector[Double](sv1.indices, sv1.values, sv1.size)
          idfs.map {
            case (id2, idf2) =>
              val sv2 = idf2.asInstanceOf[SV]
              val bsv2 = new SparseVector[Double](sv2.indices, sv2.values, sv2.size)
            //将计算出来得相关性乘以100，得到相应得权重
              val cosSim = bsv1.dot(bsv2) / (norm(bsv1) * norm(bsv2))
              (id1, id2, cosSim)
          }
      }
      docSims

    }

}
