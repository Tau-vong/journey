package com.dream

import java.sql.ResultSet


object Demo02 {
  def main(args: Array[String]): Unit = {
    val a=SQLUtils.connect()
   val sql= "INSERT INTO c VALUE(1,'ss',5);"
    val b=SQLUtils.update(sql)(a)
    print(b)
  }

}
