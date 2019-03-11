package com.lkj.realtime

import java.text.SimpleDateFormat
import java.util.Date

import com.lkj.sparkmall.common.ConfigurationBean.KafkaMessage
import com.lkj.sparkmall.common.ConfigurationUtil.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object TimeAdvClickTrendApplication {

  def main(args: Array[String]): Unit = {

    val topic = "ads_log";

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("DateAreaAdvCountTop3Application")

    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 接收数据 timestamp area city userid adid
    val DStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic,streamingContext)

    val messageDStream: DStream[KafkaMessage] = DStream.map(record => {
      val datas: Array[String] = record.value().split(" ")
      KafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
    })

    // 最近一小时广告点击量趋势
    // 使用窗口函数来实现业务要求：窗口大小为1分钟，每10秒作为滑动步长
    val windowDStream: DStream[KafkaMessage] = messageDStream\.window(Minutes(1),Seconds(10))

    // 通过窗口程序获取的数据结构：kafka
    // 将kafka的数据转换为统计的数据结构（adv_time, 1L），（adv_time, 1L），（adv_time, 1L）


      // 1，000 / 10=》0000 1L
      // 3，000 / 10=》0 1L   ==> 00~10  3L
      // 6，000 / 10=》0 1L
      // 15，000 / 10=》10000 1L
      // 18，000 / 10=》1 1L  ==> 10~20 2L
      // 22, 0000 / 10 ==> 20000 1L ==> 20~30 1L


      // 1 ==> 0~10
      // 3 ==> 0~10


      // 25 ==> 20~25

      // 25秒 / 10 = 20000

      // xxxxxxxxx01000 / 10000 + "0000"


    // 将转换结构后的数据进行聚合
    // (adv_time, sum)
    // 将聚合后的数据进行转换：(adv,(time1, sum)), (adv, (time2, sum))

    // 将聚合后的数据进行排序最多6条


    // 将最终的数据保存到Redis中


    // 4.6 将最终的数据保存到Redis中



    streamingContext.start()
    streamingContext.awaitTermination()
  }
}

