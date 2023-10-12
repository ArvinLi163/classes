package com.atguigu.spark.core.framework.common

import com.atguigu.spark.core.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/10/12 21:26
 * */
trait TApplication {
  def start(master: String = "local[*]", app: String = "Application")(op: => Unit) = {
    val conf: SparkConf = new SparkConf().setMaster(master).setAppName(app)
    val sc: SparkContext = new SparkContext(conf)
    EnvUtil.put(sc)
    try {
      op
    } catch {
      case ex => println(ex.getMessage)
    }
    sc.stop()
    EnvUtil.clear()
  }
}
