import com.alibaba.fastjson.{JSON, JSONObject}
import com.lkj.sparkmall.common.ConfigurationBean.UserVisitAction
import com.lkj.sparkmall.common.ConfigurationUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

class Application {
  def doApplication: Unit = ???
  def main(args: Array[String]): Unit = {
    doApplication
  }
  def hiveDataToRDD(spark:SparkSession): (RDD[UserVisitAction],JSONObject) ={

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

    (df.as[UserVisitAction].rdd,jsonObject)
  }
}
