package com.atguigu.spark.core.rdd.create

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/14 9:46
 * */
object Spark02_RDD_Memory_Par1 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(conf)
    //TODO 创建RDD
    //textFile作为数据源
    //defaultMinPartitions 默认最小分区数
    //math.min(defaultParallelism, 2) 默认为2
    //spark读取文件，按行读，和字节数无关
    //数据分区偏移量范围的计算,偏移量不会被重复读取
    // 1@@ =>012
    // 2@@ =>345
    // 3 =>6
    //0分区 =>[0,3]  0是起始位置即偏移量    12
    //1分区  =>[3,6] 3
    //2分区  =>[6,7]
    val rdd: RDD[String] = sc.textFile("datas/1.txt",2)
    rdd.saveAsTextFile("output")
    //TODO 关闭
    sc.stop()
  }
}
