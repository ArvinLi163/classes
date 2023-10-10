package com.atguigu.spark.core.rdd.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:累加器
 * @author: bansheng
 * @date: 2023/10/10 15:43
 * */
object Spark02_RDD_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Acc")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //获取累加器--spark提供了简单数据聚合的累加器
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")
    rdd.foreach(
      num => {
        //使用累加器
        sumAcc.add(num)
      }
    )
    //获取累加器的值
    println(sumAcc.value)

    sc.stop()
  }
}
