package com.li.video

import java.io.File
import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import java.util.{Calendar, Date, GregorianCalendar}

import com.alibaba.fastjson.JSON
import com.li.video.hdfs.Tools
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ArrayBuffer


case class LogDown(cv: String,
                   terminale: Int,
                   uname: String,
                   netclassid: Int,
                   syllabusid: Int,
                   playtime: Int,
                   playhour: Int,
                   playmonth: String,
                   playday: String,
                   playweek: Long
                  )

object LogDown {

  val fs = FileSystem.get(Tools.Configuration)
  val yyyy_mm_dd_hh_mm = new SimpleDateFormat("yyyy_MM_dd_HH_mm")
  val HH_yyyyMM_yyyyMMdd = new SimpleDateFormat("HH,yyyyMM,yyyyMMdd")
  val yyyyMMdd = new SimpleDateFormat("yyyyMMdd")


  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "root")

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val conf = new SparkConf()
      .setAppName("LogDown")
      .setMaster("local")

    val session = SparkSession.builder.config(conf).config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()

    var options: Map[String, String] = Map(
      "url" -> "jdbc:mysql://192.168.100.211:3309/htolmain_new?characterEncoding=UTF-8&transformedBitIsBoolean=false&tinyInt1isBit=false",
      "dbtable" -> "syllabus",
      "user" -> "htwx",
      "password" -> "MAin_**for^&404_mynew@@14"
    )
    // syllabus
    val syllabus = session.read.format("jdbc").options(options).load
    syllabus.createOrReplaceTempView("syllabus")

    val sc = session.sparkContext
    import session.implicits._

    val map = sc.broadcast(session.sql(" " +
      " SELECT id as syllabusId,net_class_id as netClassId from syllabus ")
      .mapPartitions {
        ite =>
          val arr = new ArrayBuffer[(Long, Long)]()

          while (ite.hasNext) {
            val r = ite.next()

            val a = r.get(0).getClass.getName match {
              case "java.lang.Integer" =>
                r.getAs[Int](0).longValue()
              case "java.lang.Long" =>
                r.getAs[Long](0)
              case _ =>
                throw new ClassCastException
            }

            val b = r.get(1).getClass.getName match {
              case "java.lang.Integer" =>
                r.getAs[Int](1).longValue()
              case "java.lang.Long" =>
                r.getAs[Long](1)
              case _ =>
                throw new ClassCastException
            }

            arr += Tuple2(a, b)
          }
          arr.iterator

      }.rdd.collectAsMap())


    val baseHdfs = "hdfs://ns1/huatu-data/video-record/flume/"
    val flumeFormat = new SimpleDateFormat("yyyy/MM/dd")

    var l = System.currentTimeMillis()

    if (args.length == 2) {
      l = args(0).toLong
    }

    if (args.length == 2 && args(1).eq("1")) {
      l = System.currentTimeMillis() - 2 * 24 * 60 ^ 60 * 1000L
    }

    val weekStart = getWeekStart(yyyyMMdd.format(new Date(l)))

    val ws = sc.broadcast(weekStart)

    val arr = new ArrayBuffer[String]()
    for (i <- 0 to 6) {

      arr += flumeFormat.format(new Date(weekStart + i * 24 * 60 * 60 * 1000))

      if (!fs.exists(new Path(baseHdfs + "" + arr(i) + ""))) {
        fs.mkdirs(new Path(baseHdfs + "" + arr(i) + ""))
      }
    }

    val videoLog = sc.textFile(baseHdfs + arr(0))
      .++(sc.textFile(baseHdfs + arr(1)))
      .++(sc.textFile(baseHdfs + arr(2)))
      .++(sc.textFile(baseHdfs + arr(3)))
      .++(sc.textFile(baseHdfs + arr(4)))
      .++(sc.textFile(baseHdfs + arr(5)))
      .++(sc.textFile(baseHdfs + arr(6)))


    val vl = videoLog

      .mapPartitions {
        ite =>
          val arr = new ArrayBuffer[LogDown]()
          val php = map.value

          val w = ws.value
          while (ite.hasNext) {

            val line = ite.next()

            val s = line.split("=")
            if (s.length > 3) {

              val cv = s(0)
              val terminal = s(1)
              val userName = s(2)

              val log = JSON.parseObject(s(3))

              val timeStr = HH_yyyyMM_yyyyMMdd.format(yyyy_mm_dd_hh_mm.parse(s(4)))
              val t = timeStr.split(",")

              val userPlayTime = log.getString("userPlayTime")
              val syllabusId = log.getString("syllabusId")

              if (syllabusId != null) {

                val netClassId = php.getOrElse(parseLong(syllabusId).get, 0L).intValue()

                arr += LogDown(
                  cv,
                  parseInt(terminal).get,
                  userName,
                  netClassId,
                  parseInt(syllabusId).get,
                  parseInt(userPlayTime).get,
                  parseInt(t(0)).get,
                  t(1),
                  t(2),
                  w)
              }
            }
          }

          arr.iterator
      }.toDF()

    vl.repartition(1)
      .write
      .partitionBy("playweek")
      .mode(SaveMode.Overwrite)
      .saveAsTable("vp")

  }

  def getWeekStart(today: String): Long = {
    var currentDate = new GregorianCalendar
    currentDate.setTime(new SimpleDateFormat("yyyyMMdd").parse(today))

    currentDate.setFirstDayOfWeek(Calendar.MONDAY)

    currentDate.set(Calendar.HOUR_OF_DAY, 0)
    currentDate.set(Calendar.MINUTE, 0)
    currentDate.set(Calendar.SECOND, 0)
    currentDate.set(Calendar.MILLISECOND, 0)
    currentDate.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    currentDate.getTime.getTime

  }

  def parseLong(s: String): Option[Long] = try {

    Some(s.toLong)
  } catch {
    case _ => None
  }

  def parseInt(s: String): Option[Int] = try {

    Some(s.toInt)
  } catch {
    case _ => None
  }

}