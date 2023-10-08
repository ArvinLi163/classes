package com.atguigu.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/10/07 16:56
 * */
object Spark07_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //行动算子 foreach
    val user = new User()
    rdd.foreach(
      num => {
        println("age = " + (user.age + num))
      }
    )
    sc.stop()
  }

  //实现序列化
  //  class User extends Serializable {
  //样例类在编译时，会自动混入序列化特质（实现可序列化接口）
  case class User() {
    var age: Int = 30
  }
}
