package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/16 9:25
 * */
object Spark19_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //连接环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)
    //TODO key-value类型
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ), 2)
    //相同的key求平均值  =>(a,3) (b,4)
    //combineByKey需要三个参数
    //第一个参数：将相同key的第一个数据进行结构的转换，实现操作
    //第二个参数：分区内的计算规则
    //第三个参数：分区间的计算规则
    val newRdd: RDD[(String, (Int, Int))] = rdd.combineByKey(
      v => (v, 1),
      (t: (Int, Int), v: Int) => {
        (t._1 + v, t._2 + 1)
      },
      (t1: (Int, Int), t2: (Int, Int)) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )
    val avgRdd: RDD[(String, Int)] = newRdd.mapValues {
      case (num, cnt) => {
        num / cnt
      }
    }
    avgRdd.collect().foreach(println)
    //关闭环境
    sc.stop()
  }
}
