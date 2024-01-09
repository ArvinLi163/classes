package com.atguigu.spark.streaming

import com.atguigu.spark.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer


/**
 * @description: 需求1：广告黑名单
 * @author: bansheng
 * @date: 2024/01/02 15:03
 * */
object SparkStreaming11_Req1_BlackList1 {
  def main(args: Array[String]): Unit = {
    //创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(conf, Seconds(3))
    //定义kafka参数
    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "lgy",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
    //kafka工具类 第二个参数是节点策略，第三个参数是消费者策略
    val consumerRecord: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("lgy"), kafkaParams))
    val adClickData: DStream[AdClickData] = consumerRecord.map(
      kafkaData => {
        val data: String = kafkaData.value()
        val datas: Array[String] = data.split(" ")
        AdClickData(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )
    //TODO 周期性获取黑名单数据
    val ds: DStream[((String, String, String), Int)] = adClickData.transform(
      rdd => {
        //TODO 通过JDBC周期性获取黑名单数据
        val blackList = ListBuffer[String]()
        val conn: Connection = JDBCUtil.getConnection
        val pstat: PreparedStatement = conn.prepareStatement("select userid from black_list")
        val rs: ResultSet = pstat.executeQuery()
        while (rs.next()) {
          blackList.append(rs.getString(1))
        }
        rs.close()
        pstat.close()
        conn.close()
        //TODO 判断点击用户是否在黑名单中
        val filterRDD: RDD[AdClickData] = rdd.filter(
          data => {
            !blackList.contains(data.user)
          })
        //TODO 如果用户不在黑名单中，那么进行统计数量（每个采集周期）
        filterRDD.map(
          data => {
            val spdf = new SimpleDateFormat("yyyy-MM-dd")
            val day = spdf.format(new Date(data.ts.toLong))
            val user = data.user
            val ad = data.ad
            ((day, user, ad), 1)
          }
        ).reduceByKey(_ + _)
      }
    )

    ds.foreachRDD(
      rdd => {
        rdd.foreach {
          case ((day, user, ad), count) => {
            println(s"${day} ${user} ${ad} ${count}")
            if (count >= 30) {
              //TODO 如果统计数量超过点击阈值(30)，那么将用户拉入到黑名单
              val conn: Connection = JDBCUtil.getConnection
              val sql=
                """
                  |insert into black_list(userid) values(?)
                  |on DUPLICATE KEY
                  |UPDATE userid = ?
                  |""".stripMargin
              //重复userid执行更新操作（行数增加）
              JDBCUtil.executeUpdate(conn,sql,Array(user,user))
              conn.close()
            } else {
              //TODO 如果没有超过阈值，那么需要将当天的广告点击数量进行更新
              //查询数据
              val conn: Connection = JDBCUtil.getConnection
              val pstat: PreparedStatement = conn.prepareStatement(
                """
                  |select
                  |    *
                  |from user_ad_count
                  |where dt =? and userid = ? and adid = ?
                  |""".stripMargin)
              pstat.setString(1,day)
              pstat.setString(2,user)
              pstat.setString(3,ad)
              val rs: ResultSet = pstat.executeQuery()
              if (rs.next()) {
                //如果存在数据，进行更新
                val pstat1: PreparedStatement = conn.prepareStatement(
                  """
                    |update user_ad_count
                    |set count = count + ?
                    |where dt =? and userid=? and adid=?
                    |""".stripMargin)
                pstat1.setInt(1, count)
                pstat1.setString(2, day)
                pstat1.setString(3, user)
                pstat1.setString(4, ad)
                pstat1.executeUpdate()
                pstat1.close()
                //TODO 判断更新后的点击数量是否超过阈值，如果超过，那么将用户拉入黑名单
                val pstat2: PreparedStatement = conn.prepareStatement(
                  """
                    |select
                    | *
                    |from user_ad_count
                    |where dt =? and userid=? and adid=? and count>=30
                    |""".stripMargin)
                pstat2.setString(1, day)
                pstat2.setString(2, user)
                pstat2.setString(3, ad)
                val rs2: ResultSet = pstat2.executeQuery()
                if (rs2.next()) {
                  val pstat3: PreparedStatement = conn.prepareStatement(
                    """
                      |insert into black_list(userid) values(?)
                      |on DUPLICATE KEY
                      |UPDATE userid =?
                      |""".stripMargin)
                  pstat3.setString(1, user)
                  pstat3.setString(2, user)
                  pstat3.executeUpdate()
                  pstat3.close()
                }
                rs2.close()
                pstat2.close()
              } else {
                //如果不存在数据，进行插入
                val pstat2: PreparedStatement = conn.prepareStatement(
                  """
                    |insert into user_ad_count(dt,userid,adid,count)
                    |values(?,?,?,?)
                    |""".stripMargin)
                pstat2.setString(1, day)
                pstat2.setString(2, user)
                pstat2.setString(3, ad)
                pstat2.setInt(4, count)
                pstat2.executeUpdate()
                pstat2.close()
              }
              rs.close()
              pstat.close()
              conn.close()
            }
          }
        }
      }
    )
    //启动采集器
    ssc.start()
    //等待采集器的关闭
    ssc.awaitTermination()
  }

  case class AdClickData(ts: String, area: String, city: String, user: String, ad: String)
}
