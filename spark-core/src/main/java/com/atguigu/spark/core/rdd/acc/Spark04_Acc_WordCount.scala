package com.atguigu.spark.core.rdd.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @description:累加器
 * @author: bansheng
 * @date: 2023/10/10 15:43
 * */
object Spark04_Acc_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Acc")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.makeRDD(List("hello", "spark", "hello"))
    //创建累加器对象
    val wcAcc = new MyAccumulator()
    //向spark注册
    sc.register(wcAcc,"wordCountAcc")
    rdd.foreach(
      //数据的累加
      word => {
        wcAcc.add(word)
      }
    )
    //获取累加器的值
    println(wcAcc.value)
    sc.stop()
  }

  /**
   * 自定义累加器:WordCount
   * 1.继承AccumulatorV2并指定泛型
   * IN:String
   * OUT:mutable.Map[String,Int]
   */
  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {
    private var wcMap = mutable.Map[String, Long]()
    //判断是否是初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator()
    }

    override def reset(): Unit = {
      wcMap.clear()
    }

    //获取累加器计算的值
    override def add(word: String): Unit = {
      val newCnt: Long = wcMap.getOrElse(word, 0L) + 1
      wcMap.update(word, newCnt)
    }

    //Driver合并多个累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = this.wcMap
      val map2 = other.value
      map2.foreach {
        case (word, count) => {
          val newCount = map1.getOrElse(word, 0L) + 1
          map1.update(word, newCount)
        }
      }
    }

    //累加器结果
    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }
}
