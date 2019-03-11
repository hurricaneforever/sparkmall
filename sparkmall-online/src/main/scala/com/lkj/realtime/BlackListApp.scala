package com.lkj.realtime

import java.util

import com.lkj.sparkmall.common.ConfigurationBean.KafkaMessage
import com.lkj.sparkmall.common.ConfigurationUtil.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

object BlackListApp {
  def main(args: Array[String]): Unit = {

    val topic = "ads_log"
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("BlackListApp")
    val streamingContext = new StreamingContext(conf, Seconds(5))

    //接收数据
    val dStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streamingContext)

    val messageDStream: DStream[KafkaMessage] = dStream.map(record => {
      val datas: Array[String] = record.value().split(" ")
      KafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
    })

    //1 将发送的数据进行验证，判断是否在黑名单列表中
    //1.1从redis中获取黑名单列表：redis:Set Smembers,sismember


    //2 如果在黑名单中，那么用户无法执行后续操作
    //3.如果不在黑名单中，那么可以继续执行业务

    /*val filterDStream: DStream[KafkaMessage] = messageDStream.filter(message => {
      !useridSetBroadcast.value.contains(message.userid)
    })*/


    val filterDStream: DStream[KafkaMessage] = messageDStream.transform(rdd => {
      val jedisClient: Jedis = RedisUtil.getJedisClient
      val useridSet: util.Set[String] = jedisClient.smembers("blackList")
      jedisClient.close()
      val useridSetBroadcast: Broadcast[util.Set[String]] = streamingContext.sparkContext.broadcast(useridSet)

      rdd.filter(message => {
        !useridSetBroadcast.value.contains(message.userid)
      })

    })

    //4.将当前用户点击的广告次数进行统计
    //4.1将用户点击广告次数在redis中进行统计：redis:Hash hincrby    ((user+ad),sum)
    //key =user:adver:clickcount, field =user+adv value =？
    filterDStream.foreachRDD(rdd => {

      rdd.foreachPartition(messages => {
        val innerJedisClient: Jedis = RedisUtil.getJedisClient
        for (message <- messages) {
          val field = message.userid + ":" + message.adid
          innerJedisClient.hincrBy("user:adver:clickcount", field, 1)

          //5.累加次数后，没有到达阈值（100），继续访问
          //5.1 获取统计次数：redis:Hash hget()
          val clickCount: Int = innerJedisClient.hget("user:adver:clickcount", field).toInt

          //6.累加次数后，到达阈值（100），将用户拉进黑名单中，禁止访问系统
          //6.1将到达阈值的用户拉入黑名单:redis:Set Set.sadd
          if (clickCount >= 100) {
            innerJedisClient.sadd("blackList", message.userid)
          }
        }
        innerJedisClient.close()
      })
    })

    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
