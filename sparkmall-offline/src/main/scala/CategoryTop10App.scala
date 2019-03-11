
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

object CategoryTop10App {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CategoryTop10App")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import spark.implicits._

    //读取动作日志数据
    val frame: DataFrame = spark.sql("use " + ConfigurationUtil.getConfigValue("hive.database"))
    //因为后续需要拼接动态条件，所以sql中增加where 1 =1
    val sql = "select * from user_visit_action where 1 = 1"

    //获取查询条件 从配置文件中获取配置信息
    val jsonConfig: String = ConfigurationUtil.getValueFromResource("condition", "condition.params.json")
    //将JSON字符串转换为JSON对象
    val jsonObject: JSONObject = JSON.parseObject(jsonConfig)

    val startDate: String = jsonObject.getString("startDate")
    val endDate: String = jsonObject.getString("endDate")

    var sqlBuilder = new StringBuilder(sql)


    if (ConfigurationUtil.isNotEmptyString(startDate)) {
      sqlBuilder.append(" and action_time >= '").append(startDate).append("'")
    }
    if (ConfigurationUtil.isNotEmptyString(startDate)) {
      sqlBuilder.append(" and action_time <= '").append(endDate).append("'")
    }
    //查询数据，将数据转换为特定类型
    val df: DataFrame = spark.sql(sqlBuilder.toString())

    val actionLogRdd: RDD[UserVisitAction] = df.as[UserVisitAction].rdd

    //使用累加器，将不同的操作的数据计算总和
    val accumulator = new CategoryCountAccumulator
    //注册累加器
    spark.sparkContext.register(accumulator, "accumulator")

    //循环RDD数据，进行累加操作
    actionLogRdd.foreach(log => {
      if (log.click_category_id != -1) {
        accumulator.add(log.click_category_id + "_click")
      } else if (log.order_category_ids != null) {
        log.order_category_ids.split(",").foreach(cid => {
          accumulator.add(cid + "_order")
        })
      } else if (log.pay_category_ids != null){
        log.pay_category_ids.split(",").foreach(cid =>{
          accumulator.add(cid +"_pay")
        })
      }
    })

    //对累加器的结果进行分组
    val statResult: Map[String, mutable.HashMap[String, Long]] = accumulator.value.groupBy {
      case (k, _) => {
        k.split("_")(0)
      }
    }


    val taskId: String = UUID.randomUUID().toString

    val infoList: List[CategoryCountInfo] = statResult.map {
      case (cid, map) => {
        CategoryCountInfo(
          taskId,
          cid,
          map.getOrElse(cid + "_click", 0L),
          map.getOrElse(cid + "_order", 0L),
          map.getOrElse(cid + "_pay", 0L)
        )
      }
    }.toList


    //sortWith 排序
    val infoes: List[CategoryCountInfo] = infoList.sortWith {
      case (left, right) => {

        if (left.clickCount > right.clickCount) {
          true
        } else if (left.clickCount == right.clickCount) {
          if (left.orderCount > right.orderCount) {
            true
          } else if (left.orderCount == right.orderCount) {
            left.payCount > right.payCount
          } else {
            false
          }
        } else {
          false
        }
      }
    }.take(10)



    //操作数据库

    val driverClass: String = ConfigurationUtil.getConfigValue("jdbc.driver.class")
    val url: String = ConfigurationUtil.getConfigValue("jdbc.url")
    val user: String = ConfigurationUtil.getConfigValue("jdbc.user")
    val password: String = ConfigurationUtil.getConfigValue("jdbc.password")

    Class.forName(driverClass)
    val connection: Connection = DriverManager.getConnection(url,user,password)
    val insertSql="insert into category_top10 values (?,?,?,?,?)"
    val pst: PreparedStatement = connection.prepareStatement(insertSql)

    infoes.foreach(info=>{
      pst.setObject(1,info.taskId)
      pst.setObject(2,info.category_id)
      pst.setObject(3,info.clickCount)
      pst.setObject(4,info.orderCount)
      pst.setObject(5,info.payCount)
      pst.executeUpdate()
    })
    pst.close()
    connection.close()
    spark.close()
  }

  class CategoryCountAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]] {
    //因为返回值是一个map，所以要先准备一个map
    private var map = new mutable.HashMap[String, Long]()

    override def isZero: Boolean = map.isEmpty

    override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
      new CategoryCountAccumulator
    }

    override def reset(): Unit = {
      new mutable.HashMap[String, Long]()
    }

    override def add(v: String): Unit = {
      map(v) = map.getOrElse(v, 0L) + 1L;
    }

    override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
      map = map.foldLeft(other.value) {
        case (xmap, (key, count)) => {
          xmap(key) = xmap.getOrElse(key, 0L) + count
          xmap
        }
      }

    }

    override def value: mutable.HashMap[String, Long] = map
  }
}

