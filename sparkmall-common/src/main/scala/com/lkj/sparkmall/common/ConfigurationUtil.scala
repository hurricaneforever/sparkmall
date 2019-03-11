package com.lkj.sparkmall.common

import java.util.{Date, Random, ResourceBundle}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import scala.collection.mutable.ListBuffer

object ConfigurationUtil {
  // 加载配置文件
  private val configBundle: ResourceBundle = ResourceBundle.getBundle("config")

  def getValueFromResource(cfgName: String, key: String): String = {
    ResourceBundle.getBundle(cfgName).getString(key)
  }

  // 从配置文件中获取资源
  def getConfigValue(key: String): String = {
    // JVM
    // 双亲委派
    // APP(classpath) => Ext(jre/lib/ext) => Startup(jre/lib,classes) ==> Ext ==> APP ==> ClassNotFoundException
    //Thread.currentThread().getContextClassLoader.getResourceAsStream("config.properties")
    configBundle.getString(key)
  }

  def isNotEmptyString(s: String): Boolean = {
    s != null && !"".equals(s.trim)
  }


  // 日期工具类
  object RandomDate {

    def apply(startDate: Date, endDate: Date, step: Int): RandomDate = {
      val randomDate = new RandomDate()
      val avgStepTime = (endDate.getTime - startDate.getTime) / step
      randomDate.maxTimeStep = avgStepTime * 2
      randomDate.lastDateTime = startDate.getTime
      randomDate
    }


    class RandomDate {
      var lastDateTime = 0L
      var maxTimeStep = 0L

      def getRandomDate() = {
        val timeStep = new Random().nextInt(maxTimeStep.toInt)
        lastDateTime = lastDateTime + timeStep

        new Date(lastDateTime)
      }
    }

  }

  // 随机生成数字
  object RandomNum {

    def apply(fromNum: Int, toNum: Int): Int = {
      fromNum + new Random().nextInt(toNum - fromNum + 1)
    }

    // 生成多个随机数
    def multi(fromNum: Int, toNum: Int, amount: Int, delimiter: String, canRepeat: Boolean) = {
      // 实现方法  在fromNum和 toNum之间的 多个数组拼接的字符串 共amount个
      //用delimiter分割  canRepeat为false则不允许重复
      //    此处逻辑请同学们自行实现
      "1,2,3"
    }

  }

  object RandomOptions {

    def apply[T](opts: RanOpt[T]*): RandomOptions[T] = {
      val randomOptions = new RandomOptions[T]()
      for (opt <- opts) {
        randomOptions.totalWeight += opt.weight
        for (i <- 1 to opt.weight) {
          randomOptions.optsBuffer += opt.value
        }
      }
      randomOptions
    }


    def main(args: Array[String]): Unit = {
      val randomName = RandomOptions(RanOpt("zhangchen", 10), RanOpt("li4", 30))
      for (i <- 1 to 40) {
        println(i + ":" + randomName.getRandomOpt())

      }
    }


  }

  object MyKafkaUtil {

    val broker_list = ConfigurationUtil.getConfigValue("kafka.broker.list")

    // kafka消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> broker_list, //用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> "commerce-consumer-group",
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
      //如果是false，会需要手动维护kafka偏移量
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    // 创建DStream，返回接收到的输入数据
    // LocationStrategies：根据给定的主题和集群地址创建consumer
    // LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
    // ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
    // ConsumerStrategies.Subscribe：订阅一系列主题

    def getKafkaStream(topic: String, ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
      val dStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))
      dStream
    }
  }

  object RedisUtil {

    var jedisPool: JedisPool = null

    def getJedisClient: Jedis = {
      if (jedisPool == null) {
        println("开辟一个连接池")
        val host = getConfigValue("redis.host")
        val port = getConfigValue("redis.port").toInt

        val jedisPoolConfig = new JedisPoolConfig()
        jedisPoolConfig.setMaxTotal(1000) //最大连接数
        jedisPoolConfig.setMaxIdle(20) //最大空闲
        jedisPoolConfig.setMinIdle(20) //最小空闲
        jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
        jedisPoolConfig.setMaxWaitMillis(500) //忙碌时等待时长 毫秒
        jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

        jedisPool = new JedisPool(jedisPoolConfig, host, port)
      }
      println(s"jedisPool.getNumActive = ${jedisPool.getNumActive}")
      println("获得一个连接")
      jedisPool.getResource
    }
  }


  case class RanOpt[T](value: T, weight: Int) {
  }

  class RandomOptions[T](opts: RanOpt[T]*) {
    var totalWeight = 0
    var optsBuffer = new ListBuffer[T]

    def getRandomOpt(): T = {
      val randomNum = new Random().nextInt(totalWeight)
      optsBuffer(randomNum)
    }
  }

}
