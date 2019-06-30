package com.optaim.util

import java.io.File

import org.ansj.library.DicLibrary
import org.ansj.recognition.impl.StopRecognition

import scala.io.Source

/**
  * ansj分词中过滤掉停用词和停用词性，并加入字典
  */

object SDDS {

 {
   val file = new File(this.getClass.getResource("/dictionarywords.txt").getPath)
    for (word <- Source.fromFile (file).getLines) {DicLibrary.insert (DicLibrary.DEFAULT, word)}
  }

  def stopNatures():StopRecognition = {
    val filter = new StopRecognition ()
    filter.insertStopNatures ("w","ag","al","vg","vl","v","f")
    filter
  }


  def stopWords():StopRecognition= {
    val sws = new java.util.ArrayList[String]
    val file1 = new File(this.getClass.getResource("/stopwords.txt").getPath)
    for (word <- Source.fromFile(file1).getLines) {
      sws.add(word)
    }
    val filter = new StopRecognition ()
    filter.insertStopWords(sws)
    filter
  }

}
