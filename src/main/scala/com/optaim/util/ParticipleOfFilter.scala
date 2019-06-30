package com.optaim.util

/**
  * 过滤掉ansj分词中长度过小的词并输出目标格式的方法抽取的类
  */

object ParticipleOfFilter {

  //对文本做余弦相似性或求TFIDF处理前的分词进行过滤，将文本之间处理成空格，将长度为1或不为汉字的过滤掉
  def ansjFilter(x:(String,String)):(String,String)= {
    var str = ""
    val value1 = x._2.split("\\|")
    for (value2 <- value1) {
      if (value2.length > 1 && value2.matches("[\u4e00-\u9fa5]+")) {
        if (str == "") {
          str = value2
        }
        else {
          str = str + " " + value2
        }
      }
    }
    (x._1, str)
  }

  //对景点分词在求TFIDF之前进行过滤，过滤掉长度不符合和格式不符合的分词
  def ansjFilter1(x:(String,String,String,String)):(String,String,String,String) = {
    var str = ""
    val value1 = x._4.split("\\|")
    for (value2 <- value1) {
      if (value2.length > 1 && value2.matches("[\u4e00-\u9fa5]+")) {
        if (str == "") {
          str = value2
        }
        else {
          str = str + " " + value2
        }
      }
    }
    (x._1,x._2,x._3, str)
  }

}
