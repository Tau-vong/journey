package com.optaim.util

/**
  * 将景点分词分装成（key，value）的形式的方法抽取的类
  */

object ParameterOfCos {

  def separate(x:String):(String,Seq[String])= {
    val a = x.substring(1, x.length - 1)
    val key = a.split(" ")(0)
    val value = a.split(" ").toSeq
    (key, value)
  }
}
