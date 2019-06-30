package com.dream

import java.sql.{Connection, DriverManager, Statement}

object SQLUtils {

  //  val postgprop = new Properties()
  //  val ipstream: InputStream = this.getClass().getResourceAsStream("/config.properties")
  //  postgprop.load(ipstream)
  //  postgprop.get("driver")

  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://localhost/mydb1"
  val username = "root"
  val password = "root"


  def connect(): Connection = {

  Class.forName(driver)
  val connection = DriverManager.getConnection(url, username, password)
  connection
}


  def update(sql:String)(conn:Connection):Int={
    var statement: Statement = null
    try {
      statement = conn.createStatement()
      statement.executeUpdate(sql)
    } finally {
      if (conn != null) conn.close()
      if (statement != null) statement.close()
    }

  }

}
