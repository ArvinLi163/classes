package com.atguigu.spark.core.framework.application

import com.atguigu.spark.core.framework.common.TApplication
import com.atguigu.spark.core.framework.controller.WordCountController


/**
 * @description:
 * @author: bansheng
 * @date: 2023/10/12 20:57
 * */
object WordCountApplication extends App with TApplication {
  start() {
    val controller = new WordCountController()
    controller.dispatch()
  }
}
