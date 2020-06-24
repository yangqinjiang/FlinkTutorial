package com.atguigu.apitest

import com.atguigu.apitest.SourceTest.getClass
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    //环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置流时间特征：事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //默认值
    env.getConfig.setAutoWatermarkInterval(100L)
    //2种方式读取源 时间语义： 处理时间
    //1, 从文件读取
    //val stream = env.readTextFile(getClass.getResource("/sensor.txt").getPath)
    //2 , 通过socket生成数据 nc -lp 7777
    val stream = env.socketTextStream("localhost",7777)
    val dataStream = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    })

//      .assignAscendingTimestamps(_.timestamp*1000L)
      //.assignTimestampsAndWatermarks(new MyAssigner())
      //常用的写法
      .assignTimestampsAndWatermarks(
        // 延迟时间 1s
        new BoundedOutOfOrdernessTimestampExtractor[SensorReading]( Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = {
          t.timestamp * 1000
        }
      })

    //统计10s内的最小温度
    val minTempPerWindowsStream = dataStream
      .map( data => (data.id,data.temperature))
        .keyBy(_._1)
//        .timeWindow(Time.seconds(10))//开滚动时间窗口,事件时间
      .timeWindow(Time.seconds(10),Time.seconds(5))//开滑动时间窗口,事件时间
        .reduce((data1,data2)=>(data1._1,data1._2.min(data2._2))) //用reduce做增量聚合
    minTempPerWindowsStream.print("min Temp ")
    dataStream.print("input data")

    env.execute("window test")
  }
}
//水位线,周期性
class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading]{
  val bound = 60000
  var maxTs = Long.MinValue
  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound)
  }

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    maxTs = maxTs.max(t.timestamp * 1000)
    t.timestamp * 1000
  }
}
//水位线,非周期性
class MyAssigner2() extends AssignerWithPunctuatedWatermarks[SensorReading]{
  override def checkAndGetNextWatermark(t: SensorReading, extractTimestamp: Long): Watermark = {
    new Watermark(extractTimestamp)
  }

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    t.timestamp * 1000
  }
}