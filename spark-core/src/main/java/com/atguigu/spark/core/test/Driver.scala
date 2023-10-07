package com.atguigu.spark.core.test

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/13 10:19
 * */
object Driver {
  def main(args: Array[String]): Unit = {
    //客户端进行连接
    val client1 = new Socket("localhost", 9999)
    val client2 = new Socket("localhost", 8888)
    //以输出流的形式向服务端发送数据
    val outputStream1: OutputStream = client1.getOutputStream
    val outputStream2: OutputStream = client2.getOutputStream

    //创建一个Task对象
    val task = new Task()
    val subTask1 = new SubTask()
    subTask1.datas = task.datas.take(2)
    subTask1.logic = task.logic
    //对象输出流
    val objOut1 = new ObjectOutputStream(outputStream1)
    objOut1.writeObject(subTask1)
    objOut1.flush()
    //关流
    objOut1.close()
    //关闭客户端的连接
    client1.close()


    val subTask2 = new SubTask()
    subTask2.datas = task.datas.takeRight(2)
    subTask2.logic = task.logic
    //对象输出流
    val objOut2 = new ObjectOutputStream(outputStream2)
    objOut2.writeObject(subTask2)
    objOut2.flush()
    //关流
    objOut2.close()
    //关闭客户端的连接
    client2.close()
  }
}
