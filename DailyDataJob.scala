package com.bianfeng.bigdata.dws

import java.io.File
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalAdjusters.firstDayOfMonth
import java.time.{LocalDate, ZoneId}
import java.util.Calendar

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.{GlueArgParser, Job}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Created by wangning on 2019/12/3 16:56.
 */

object DailyDataJob {


  var isOnline: Boolean = true //参数决定维度数据是否上线
  var startDate: String = "default"
  var endDate: String = "default"
  var numOfDays: Long = 1
  var amount: Int = -1
  var debug: Boolean = true //打印调试信息
  var mode: String = _ // 数据更新逻辑
  var partition: Int = 0
  var isRefresh: Boolean = false // 是否需要重新刷历史数据
  var paramMap: mutable.Map[String, Any] = mutable.Map[String, Any]() //其他非命令行参
  val readFormat = new SimpleDateFormat("yyyyMMdd")
  /**
   * ods层表名
   */
  var odsTableName: String = _

  var spark: SparkSession = null
  var sc: SparkContext = null
  var gc: GlueContext = null
  val ODS_THRESHOLD_VALUE = 640000

  var todayDf: DataFrame = null
  var yesterdayDf: DataFrame = null
  var firstDayMonthDf: DataFrame = null

  /**
   * init date
   */
  var startFormat: LocalDate = null
  var endFormat: LocalDate = null
  var yesterday: String = null
  var firstDayMonth: String = null

  /**
   * 程序入口
   *
   * @param sysArgs
   */
  def main(sysArgs: Array[String]) {

    val calendar = Calendar.getInstance()
    odsTableName = "daily_data"
    try {
      println("init start ....")
      init()
      println("init end ....")

      beforeExecute()
      println("execute start ....")
      val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "startDate", "endDate", "amount").toArray)
      Job.init(args("JOB_NAME"), gc, args.asJava)
      startDate = args.get("startDate").get
      endDate = args.get("endDate").get
      amount = args.get("amount").get.toInt


      initDate()
      if (startFormat != null && endFormat != null) {
        numOfDays = startFormat.until(endFormat, ChronoUnit.DAYS)
      }
      calendar.setTime(readFormat.parse(startDate))
      while (numOfDays >= 0) {
        execute()
        calendar.add(Calendar.DAY_OF_MONTH, 1)
        startDate = readFormat.format(calendar.getTime())
        initDate()
        numOfDays = numOfDays - 1
        println("startDate:" + startDate + "，numOfDays：" + numOfDays)
      }
      println("execute end ....")
    } catch {
      case e: Exception => throw e
    } finally {
      destroy()
    }
  }

  /**
   * 全局变量初始化
   */
  def init(): Unit = {
    spark = SparkSession.builder()
      .config("spark.driver.allowMultipleContexts", "true")
      .getOrCreate()
    sc = spark.sparkContext
    gc = new GlueContext(sc)
  }

  def beforeExecute(): Unit = {

  }

  /**
   * ETL过程执行程序
   */
  def execute(): Unit = {

    extract()

    val result = transform()

    load(result, odsTableName)
  }

  /**
   * release resource
   */
  def destroy(): Unit = {
    if (sc != null) {
      try {
        gc.clearCache()
      } catch {
        case e: Throwable =>
      }
      sc.stop()
    }
  }

  /**
   * 源数据读取函数, ETL中的Extract
   * 如需自定义，可以在子类中重载实现
   *
   * @return
   */
  def extract() = {
    readSource()
  }

  def readSource(): Unit = {
    todayDf = readFromDws(gc, startDate, startDate)
    yesterdayDf = readFromDws(gc, yesterday, yesterday)
    firstDayMonthDf = readFromDws(gc, firstDayMonth, startDate)
  }

  /**
   * 数据转换函数，ETL中的Transform
   *
   * @return
   */
  def transform(): DataFrame = {
    //注册临时表
    todayDf.createOrReplaceTempView("todayTable")
    yesterdayDf.createOrReplaceTempView("yesterdayTable")
    firstDayMonthDf.createOrReplaceTempView("firstTable")

    val result = gc.sql(
      """
	    |select a.app_id,a.app_name,a.country,a.platform,a.revenue,a.impression,a.dnu,a.dnu,a.spend,a.af_organ_install,a.dnu_dau,a.imps_dau,a.ecpm,a.ads_arpu,
        |a.organ_install,a.cpi,a.roi,b.day_spend,b.day_dau,c.month_revenue,c.month_spend,a.day,a.rank,b.day_revenue,d.yester_revenue,
        |a.revenue/b.day_revenue as dod,(d.yester_revenue-a.revenue)/d.yester_revenue as mom
        |from(select k.*,rank() over(partition by k.day_p,k.app_id,k.platform order by k.revenue desc) as rank
        |  from(
        |select day_p,day,app_name,app_id,country,platform,sum(revenue)as revenue,sum(impression) as impression, avg(dau) as dau, avg(dnu) as dnu,
        |avg(spend) as spend,avg(af_organic_install) as af_organ_install,
        |avg(dnu)/avg(dau) as dnu_dau,
        |sum(impression)/avg(dau) as imps_dau,
        |sum(revenue)/sum(impression)*1000 as ecpm,
        |sum(revenue)/avg(dau) as ads_arpu,
        |avg(af_organic_install)/avg(dnu) as organ_install,
        |avg(spend)/avg(tf_installs) as cpi,
        |sum(revenue)/avg(spend) as roi
        |FROM todayTable
        |group by  day_p,day,app_id,app_name,country,platform) k) a
        |left join
        |(select day_p,day,app_id,platform,sum(revenue)as day_revenue,avg(spend_sum) as day_spend,avg(dau_sum) as day_dau
        |FROM todayTable
        |group by  day_p,day,app_id,platform) b
        |on a.day_p = b.day_p and a.platform=b.platform and a.app_id = b.app_id
        |left join(
        |    SELECT  substr(day_p,1,6) as month,app_id,app_name,platform,sum(revenue)as month_revenue,sum(spend)  as month_spend
        |    FROM(
        |      SELECT  day_p,app_id,app_name,platform,sum(revenue)as revenue,avg(spend_sum)  as spend
        |      FROM firstTable
        |      group by day_p,app_id,app_name,platform)
        |    group by substr(day_p,1,6),app_id,app_name,platform)  c
        |on a.app_id = c.app_id and a.platform=c.platform
        |left join(
        |select app_id,country,platform,sum(revenue)as yester_revenue
        |FROM yesterdayTable
        |group by  day_p,day,app_id,app_name,country,platform) d
        |on a.app_id = d.app_id and a.country = d.country and a.platform=d.platform

        """.stripMargin)

    println("最终结果数据处理完成。。。。")
    if (debug) {
      result.show(10)
    }
    result
  }

  /**
   * 数据存储函数，ETL中的Load
   */
  def load(df: DataFrame, topicName: String) = {
    backup(df, topicName)
  }

  /**
   * 数据备份处理
   *
   * @param df
   * @param topicName
   */
  def backup(df: DataFrame, topicName: String): Unit = {
    val S3_BASE_PATH = "s3://top1--bigdata/data_warehouse"
    val DWS_BASE_PATH = S3_BASE_PATH + "/dws_job"
    val partitionParentPath = File.separator + "day_p=" + startDate
    val onLineOdsDir = DWS_BASE_PATH + File.separator + topicName + partitionParentPath

    println("线上数据目录:" + onLineOdsDir)

    //防止文件碎片
    val lineCount = df.count
    val total_count = BigDecimal(lineCount)
    val load_to_s3_partition = (total_count / ODS_THRESHOLD_VALUE).intValue + 1
    println("load_to_s3_partition:" + load_to_s3_partition)

    if (isOnline) {
      df.repartition(load_to_s3_partition).write.mode(SaveMode.Overwrite).parquet(onLineOdsDir)
      println("数据上线成功 。。。。")
    }
  }

  /**
   * 获取时间
   */
  def initDate(): Unit = {
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    if (!startDate.equals("default")) {
      startFormat = readFormat.parse(startDate).toInstant.atZone(ZoneId.systemDefault()).toLocalDate
    } else {
      startFormat = LocalDate.now().plusDays(amount)
      startDate = startFormat.format(formatter)
    }
    if (!endDate.equals("default")) {
      endFormat = readFormat.parse(endDate).toInstant.atZone(ZoneId.systemDefault()).toLocalDate
    } else {
      endFormat = LocalDate.now().plusDays(-1)
      endDate = endFormat.format(formatter)
    }
    yesterday = startFormat.plusDays(-1).format(formatter)
    firstDayMonth = startFormat.`with`(firstDayOfMonth).format(formatter)

    println("yesterday:" + yesterday)
    println("firstDayMonth" + firstDayMonth)
    println("执行时间为：" + startDate)
  }

  /**
   * 读取原始层数据源
   *
   * @param glueContext
   * @param startDate 时间
   * @return
   */
  def readFromDws(glueContext: GlueContext, startDate: String, endDate: String): DataFrame = {
    val sql = s"select * from dws_job.dws_country_spend_revenue_iap where day_p between '$startDate' and  '$endDate'"
    val sourceDf = glueContext.sql(sql)
    sourceDf
  }

}
