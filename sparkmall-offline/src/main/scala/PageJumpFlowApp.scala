
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lkj.sparkmall.common.ConfigurationBean.{CategoryCountInfo, UserVisitAction}
import com.lkj.sparkmall.common.ConfigurationUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

object PageJumpFlowApp extends Application{
  override def doApplication: Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CategoryTop10App")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val (logRDD,jsonObject)= hiveDataToRDD(spark)

    doServer(spark,jsonObject,logRDD)
  }

  def doServer(spark:SparkSession,jsonObject:JSONObject,actionLogRDD: RDD[UserVisitAction]): Unit = {


    //  1.将日志数据进行筛选过滤，辅助跳转转换的页面ID(1-7)
    //      此处过滤会有问题，会影响页面流转顺序，所以此处的过滤应该放在统计页面点击数量时使用
    val pageids: Array[String] = jsonObject.getString("targetPageFlow").split(",")


    //    2.1将数据通过session进行分组
    val sessionGroupRDD: RDD[(String, Iterable[UserVisitAction])] = actionLogRDD.groupBy(logs => logs.session_id)

    //    2.2将过滤后的数据进行排序，按照点击的时间排序
    //  [sessionid,List[UserVisitAction]]
    //  [sessionid,List(pageflow,count)]
    //  List(pageflow,count)
    //  (pageflow,count)
    val zipLogRDD: RDD[(String, Int)] = sessionGroupRDD.mapValues(datas => {
      val sortLogs: List[UserVisitAction] = datas.toList.sortWith {
        case (left, right) => {
          left.action_time < right.action_time
        }
      }
      val sortPageids: List[Long] = sortLogs.map(log => log.page_id)
      sortPageids.zip(sortPageids.tail).map {
        case (pageid1, pageid2) => {
          (pageid1 + "-" + pageid2, 1)
        }
      }
    }).map(_._2).flatMap(x => x)


    //    3.将筛选后的数据根据页面ID进行分组聚合统计得到点击次数(A)
    val filterPageRDD: RDD[UserVisitAction] = actionLogRDD.filter(log => {
      pageids.contains("" + log.page_id)
    })
    val pageSumClickRDD: RDD[(Long, Int)] = filterPageRDD.map(log => (log.page_id, 1)).reduceByKey(_ + _)
    val pageSumClickMap: Map[Long, Int] = pageSumClickRDD.collect().toMap

    //    4.将页面转换的ID进行拉链(zip)操作(12, 23,34)
    val zipPageArray: Array[String] = pageids.zip(pageids.tail).map {
      case (pageid1, pageid2) => {
        pageid1 + "-" + pageid2
      }
    }

    //    5.将筛选后的数据进行拉链，然后和我们需要的页面转换拉链数据进行作比较
    //    5.1先过滤
    val zipLogFilterRDD: RDD[(String, Int)] = zipLogRDD.filter(zipLog => {
      zipPageArray.contains(zipLog._1)
    })
    //    6.将过滤的数据进行分组聚合统计点击次数(B)
    val zipLogReduceRDD: RDD[(String, Int)] = zipLogFilterRDD.reduceByKey(_ + _)

    val taskId: String = UUID.randomUUID().toString
    //    7.将A-B/A结果保存到MySQL数据库
    zipLogReduceRDD.foreach {
      case (pageflow, sum) => {
        val a: String = pageflow.split("-")(0)
        val aCount = pageSumClickMap(a.toLong)

        val result = (sum.toDouble / aCount * 100).toLong
        println(pageflow + ":" + result)
      }
    }



    //操作数据库
    //    val driverClass: String = ConfigurationUtil.getConfigValue("jdbc.driver.class")
    //    val url: String = ConfigurationUtil.getConfigValue("jdbc.url")
    //    val user: String = ConfigurationUtil.getConfigValue("jdbc.user")
    //    val password: String = ConfigurationUtil.getConfigValue("jdbc.password")
    //
    //    Class.forName(driverClass)
    //    val connection: Connection = DriverManager.getConnection(url,user,password)
    //    val insertSql="insert into category_top10 values (?,?,?,?,?)"
    //    val pst: PreparedStatement = connection.prepareStatement(insertSql)
    //
    //    infoes.foreach(info=>{
    //      pst.setObject(1,info.taskId)
    //      pst.setObject(2,info.category_id)
    //      pst.setObject(3,info.clickCount)
    //      pst.setObject(4,info.orderCount)
    //      pst.setObject(5,info.payCount)
    //      pst.executeUpdate()
    //    })
    //    pst.close()
    //    connection.close()
       spark.close()
  }


}

