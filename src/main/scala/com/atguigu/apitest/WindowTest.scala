package com.atguigu.apitest

import com.atguigu.apitest.SourceTest.getClass
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    //环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //2种方式读取源 时间语义： 处理时间
    //1, 从文件读取
    //val stream = env.readTextFile(getClass.getResource("/sensor.txt").getPath)
    //2 , 通过socket生成数据 nc -lp 7777
    val stream = env.socketTextStream("localhost",7777)
    val dataStream = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    })

    //统计10s内的最小温度
    val minTempPerWindowsStream = dataStream
      .map( data => (data.id,data.temperature))
        .keyBy(_._1)
        .timeWindow(Time.seconds(10))//开时间窗口
        .reduce((data1,data2)=>(data1._1,data1._2.min(data2._2))) //用reduce做增量聚合
    minTempPerWindowsStream.print("min Temp ")
    dataStream.print("input data")

    env.execute("window test")
  }
}
