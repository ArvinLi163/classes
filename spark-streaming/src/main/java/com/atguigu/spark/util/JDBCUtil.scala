package com.atguigu.spark.util

import com.alibaba.druid.pool.DruidDataSourceFactory

import java.sql.{Connection, PreparedStatement}
import java.util.Properties
import javax.sql.DataSource

/**
 * @description:
 * @author: bansheng
 * @date: 2024/01/09 14:26
 * */
object JDBCUtil {

  //初始化数据库连接池
  val dataSource: DataSource = init()

  //初始化连接池方法
  def init(): DataSource = {
    val properties = new Properties()
    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("url", "jdbc:mysql://hadoop102:3306/spark_streaming?useUnicode=true&characterEncoding=UTF-8")
    properties.setProperty("username", "root")
    properties.setProperty("password", "123456")
    properties.setProperty("maxActive", "50")
    DruidDataSourceFactory.createDataSource(properties)
  }
  //连接mysql
  def getConnection:Connection ={
    dataSource.getConnection
  }

  //执行 SQL 语句,单条数据插入
  def executeUpdate(connection: Connection, sql: String, params: Array[Any]): Int = {
    var rtn = 0
    var pstmt: PreparedStatement = null
    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- params.indices) {
          pstmt.setObject(i + 1, params(i))
        }
      }
      rtn = pstmt.executeUpdate()
      connection.commit()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }
}
