package com.lkj.realtime

import java.text.SimpleDateFormat
import java.util.Date

import com.lkj.sparkmall.common.ConfigurationBean.KafkaMessage
import com.lkj.sparkmall.common.ConfigurationUtil.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.JsonMethods
import redis.clients.jedis.Jedis

object DateAreaAdvTop3App {
  def main(args: Array[String]): Unit = {

    val topic = "ads_log"
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DateAreaCityAdvCountApp")
    val streamingContext = new StreamingContext(conf, Seconds(5))

    //接收数据
    val dStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streamingContext)

    val messageDStream: DStream[KafkaMessage] = dStream.map(record => {
      val datas: Array[String] = record.value().split(" ")
      KafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
    })

    //timestamp + " " + area + " " + city + " " + userid + " " + adid
    //需求：  每天各地区 top3 热门广告
    //1.将kafka获取的数据进行拆分 (date:area:city:adid,1)

    val dateAreaCityAdsDStream: DStream[(String, Long)] = messageDStream.map(message => {
      val date: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(message.timestamp.toLong))
      val key = date + ":" + message.area + ":" + message.city + ":" + message.adid
      (key, 1L)
    })

    streamingContext.sparkContext.setCheckpointDir("cp")
    //2.将拆分数据进行聚合统计(date:area:city:ads,sum)
    //seq是Long类型的 ByKey当key相同时Long才会形成的序列，key相同，多个Long放在一起，聚合在一起，类似于分组，但是还没有聚合
    //option 作聚合时，是每一段相加，那么前一段累加的值存在检查点，检查点当中存的值存在option里，option是检查点
    //当中同一个key存的值就是当前的option, option 其实就是一个第三方的存储点，先去判断存储点option有没有值，没有值默认值是0L，
    //然后开始累加第一段，得到sum，再将sum代入Option，将sum存到了检查点中
    val totalClickDStream: DStream[(String, Long)] = dateAreaCityAdsDStream.updateStateByKey {
      case (seq, cache) => {
        val sum = cache.getOrElse(0L) + seq.sum
        Option(sum)
      }
    }
//    //3.将聚合的结果保存到检查点(有状态)中，保存到Redis中
//    totalClickDStream.foreachRDD(rdd=>{
//      rdd.foreachPartition(datas=>{
//        val jedisClient: Jedis = RedisUtil.getJedisClient
//
//        for((key,sum)<-datas){
//          jedisClient.hset("date:area:city:ads,sum",key,sum.toString)
//        }
//
//        jedisClient.close()
//      })
//    })




    //(date:area:city:adid,sum)
    val dateAreaCityAdsReduceDStream: DStream[(String, Long)] = totalClickDStream.reduceByKey(_ + _)

    //2.将数据进行结构转换(date:area:ads,sum1) (date:area:ads,sum2)
    val dateAreaAdsSumDStream: DStream[(String, Long)] = dateAreaCityAdsReduceDStream.map {
      case (key, sum) => {
        val ks = key.split(":")
        val newKey = ks(0) + ":" + ks(1) + ":" + ks(3)
        (newKey, sum)
      }
    }

    //3.将转换后的数据进行聚合统计(date:area:ads,sumTotal)
    val dateAreaAdsReduceDStream: DStream[(String, Long)] = dateAreaAdsSumDStream.reduceByKey(_ + _)

    //4.转换结构((date:area),(ads1,sumTotal)),((date:area),(ads2,sumTotal))
    val dateAreaToAdsSumDStream: DStream[(String, (String, Long))] = dateAreaAdsReduceDStream.map {
      case (key, sum) => {
        val ks = key.split(":")
        val newKey = ks(0) + ":" + ks(1)
        val newVal = (ks(2), sum)
        (newKey, newVal)
      }
    }
    //5.将转换后的数据聚合 然后进行排序获取TOP3
    val dateAreaGroupAdsSumDStream: DStream[(String, Iterable[(String, Long)])] = dateAreaToAdsSumDStream.groupByKey()

    val resultDStream: DStream[(String, List[(String, Long)])] = dateAreaGroupAdsSumDStream.mapValues(datas => {
      datas.toList.sortWith {
        case (left, right) => {
          left._2 > right._2
        }
      }.take(3)
    })

    val resultMapDStream: DStream[(String, Map[String, Long])] = resultDStream.map {
      case (key, list) => {
        (key, list.toMap)
      }
    }

    import org.json4s.JsonDSL._
    //6.将最终的数据保存到Redis中
    resultMapDStream.foreachRDD(rdd => {
      rdd.foreachPartition(datas => {
        val jedisClient: Jedis = RedisUtil.getJedisClient

        for ((key,map) <- datas) {
          val ks =key.split(":")
          val value: String = JsonMethods.compact(JsonMethods.render(map))
          jedisClient.hset("top3_ads_per_day:"+ks(0),ks(1),value)

        }
        jedisClient.close()
      })


    })


    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
