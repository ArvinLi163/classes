package com.atguigu.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/10/09 16:18
 * */
object Spark05_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    /**  cache:将数据临时存储在内存中进行数据重用
     *        会在血缘关系中添加新的依赖，一旦出现问题，就会重新读取数据
     * persist:将数据临时存储在磁盘文件中进行数据重用，涉及到磁盘IO，性能较低，但是数据安全
     *        如果作业执行完毕，临时保存的数据文件就会丢失
     * checkpoint:将数据长久地保存在磁盘磁盘文件中进行数据重用，涉及磁盘IO，性能较低，但是数据安全，
     *            为了办证数据安全，所以一般情况下，会独立执行作业，为了提高效率，一般情况下，是需要和cache联合使用
     */
    //设置检查点路径
    sc.setCheckpointDir("cp")

    val list: List[String] = List("Hello Spark", "Hello Scala")
    val RDD: RDD[String] = sc.makeRDD(list)
    val flatRDD: RDD[String] = RDD.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = flatRDD.map(word => {
      println("@@@@@@@@@@@@@@@@@@@@")
      (word, 1)
    })
    mapRDD.cache()
    //检查点要落盘
    mapRDD.checkpoint()
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println("*********************")
    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)
    sc.stop()
  }
}
