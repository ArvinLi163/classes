package com.atguigu.spark.core.test

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/13 16:18
 * */
class SubTask extends Serializable {
  var datas: List[Int] = _
  var logic: Int => Int = _

  //计算
  def compute(): List[Int] = {
    datas.map(logic)
  }
}
