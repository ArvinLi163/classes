package com.atguigu.spark.core.framework.common

import com.atguigu.spark.core.framework.util.EnvUtil
import org.apache.spark.rdd.RDD

/**
 * @description:
 * @author: bansheng
 * @date: 2023/10/12 21:45
 * */
trait TDao {
  def readFile(path: String): RDD[String] = {
    EnvUtil.take().textFile(path)
  }
}
