package com.atguigu.spark.core.framework.service


import com.atguigu.spark.core.framework.common.TService
import com.atguigu.spark.core.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
 * @description:服务层
 * @author: bansheng
 * @date: 2023/10/12 20:59
 * */
class WordCountService extends TService {
  private val dao = new WordCountDao()

  //数据分析
  def dataAnalysis() = {
    val lines: RDD[String] = dao.readFile("datas/word.txt")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))
    val groupCount: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(t => t._1)
    val count: RDD[(String, Int)] = groupCount.map {
      case (word, list) => list.reduce((t1, t2) => (t1._1, t1._2 + t2._2))
    }
    val array: Array[(String, Int)] = count.collect()
    array
  }
}
