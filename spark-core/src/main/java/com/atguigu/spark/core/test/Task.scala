package com.atguigu.spark.core.test

/**
 * @description:
 * 实现序列化
 * @author: bansheng
 * @date: 2023/09/13 11:00
 * */
class Task extends Serializable {
  //数据
  val datas = List(1, 2, 3, 4)
  //  val logic = (num: Int) => {
  //    num * 2
  //  }
  val logic: Int => Int = _ * 2

  //计算
  def compute(): List[Int] = {
    datas.map(logic)
  }
}
