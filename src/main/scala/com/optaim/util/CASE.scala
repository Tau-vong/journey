package com.optaim.util

/**
  * 样例类
  */

object CASE {
  case class RawDataRecord(category: String, text: String)
  case class RawDataRecord1(category1: String, category2: String,category: String,text: String)
  case class CityData(city:String,keyWord:String,weight:String)
  case class SightData(sight:String,viewId:String,city:String,keyWord:String,weight:String)
  case class MaxCityData(city:String,maxData:String)
  case class MaxSightData(sight:String,maxData:String)

  //TODO
  case class SightData1(keyWord:String,weight:String,sight:String)
  case class CosData(sight:String,cosSight:String,cos:String)

}
