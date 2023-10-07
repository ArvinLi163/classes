package com.atguigu.spark.core.test

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/13 10:19
 * */
object Executer2 {
  def main(args: Array[String]): Unit = {
    //启动服务器
    val server2 = new ServerSocket(8888)
    println("服务器启动，等待接收数据")
    //等待接收客户端发来的数据
    val client2: Socket = server2.accept()
    //以输入流的形式接收数据
    val inputStream2: InputStream = client2.getInputStream

    //对象输入流
    val objIn2 = new ObjectInputStream(inputStream2)

    //读取数据
    val task2: SubTask = objIn2.readObject().asInstanceOf[SubTask]
    val ints2: List[Int] = task2.compute()
    println("已接收到来自客户端[8888]的数据：" + ints2)
    objIn2.close()
    client2.close()
    server2.close()
  }
}
