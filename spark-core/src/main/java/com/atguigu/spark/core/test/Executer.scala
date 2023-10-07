package com.atguigu.spark.core.test

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/13 10:19
 * */
object Executer {
  def main(args: Array[String]): Unit = {
    //启动服务器
    val server = new ServerSocket(9999)
    println("服务器启动，等待接收数据")
    //等待接收客户端发来的数据
    val client1: Socket = server.accept()
    //以输入流的形式接收数据
    val inputStream1: InputStream = client1.getInputStream

    //对象输入流
    val objIn1 = new ObjectInputStream(inputStream1)

    //读取数据
    val task1: SubTask = objIn1.readObject().asInstanceOf[SubTask]
    val ints1: List[Int] = task1.compute()
    println("已接收到来自客户端[9999]的数据：" + ints1)
    objIn1.close()
    client1.close()
    server.close()
  }
}
