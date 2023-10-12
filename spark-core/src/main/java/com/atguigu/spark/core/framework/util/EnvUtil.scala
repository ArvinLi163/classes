package com.atguigu.spark.core.framework.util

import org.apache.spark.SparkContext

/**
 * @description:
 * @author: bansheng
 * @date: 2023/10/12 21:53
 * */
object EnvUtil {
  //ThreadLocal可以对线程的内存进行控制，存储数据，共享数据
  private val scLocal = new ThreadLocal[SparkContext]()

  def put(sc: SparkContext) = {
    scLocal.set(sc)
  }

  def take() = {
    scLocal.get()
  }
  def clear()={
    scLocal.remove()
  }
}
