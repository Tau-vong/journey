package com.dream

import java.sql.{DriverManager, Connection, ResultSet}
object Spark2Mysql00 {

  val user="root"
  val password = "root"
  val host="localhost"
  val database="mydb1"
  val conn_str = "jdbc:mysql://"+host +":3306/"+database+"?user="+user+"&password=" + password
  println(conn_str)


  def main(args:Array[String]): Unit ={
    //classOf[com.mysql.jdbc.Driver]
    Class.forName("com.mysql.jdbc.Driver").newInstance();
    val conn = DriverManager.getConnection(conn_str)
    println("hello")
    try {
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      // Execute Query
      val rs = statement.executeQuery("SHOW TABLES")

      // Iterate Over ResultSet
      while (rs.next) {
        println(rs.getRow())
      }
    }
    catch {
      case _ : Exception => println("===>")
    }
    finally {
      conn.close
    }
  }
}