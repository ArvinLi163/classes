package com.atguigu.spark.core.framework.controller

import com.atguigu.spark.core.framework.common.TController
import com.atguigu.spark.core.framework.service.WordCountService

/**
 * @description:控制层
 * @author: bansheng
 * @date: 2023/10/12 20:58
 * */
class WordCountController extends TController{
  private val service = new WordCountService()

  //调度
  def dispatch() = {
    val array: Array[(String, Int)] = service.dataAnalysis()
    array.foreach(println)
  }
}
