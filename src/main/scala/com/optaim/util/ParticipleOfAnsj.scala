package com.optaim.util

import org.ansj.splitWord.analysis.DicAnalysis

/**
  * 进行ansj中文分词的方法抽取的类
  */

object ParticipleOfAnsj {

  //对进行文本相似度计算的景点描述做分词处理
  def ansjMethod(x:String):(String,String) ={

    val key = x.split("\t")(4)
    val va = (x.split("\t")(x.split("\t").length - 1))
    val value=participle(va)
    (key,value)
  }

  //对城市文本进行有用字段截取
  def ansjMethod0(x:String):(String,String) ={

    val key = x.split("\t")(3)
    val value = (x.split("\t")(x.split("\t").length - 1))
    (key,value)
  }



  //按景点对景点描述做分词处理
  def ansjMethod1(x:String):(String,String,String,String)={
    val k0 = x.split("\t")(0)
    val k1 = x.split("\t")(3)
    val key = x.split("\t")(4)
    val va = (x.split("\t")(x.split("\t").length - 1))
    val value=participle(va)
    //val value=va1.split("\\|")

    (k0,k1,key, value)
  }

  //ANSJ做分词
  def participle(x:String):String={
    val value = DicAnalysis.parse(x).recognition(SDDS.stopNatures).recognition(SDDS.stopWords()).toStringWithOutNature("|")
    value
  }

}
