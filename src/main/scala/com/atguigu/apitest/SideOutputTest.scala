package com.atguigu.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object SideOutputTest {
  def main(args: Array[String]): Unit = {
    //环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置流时间特征：事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //2种方式读取源 时间语义： 处理时间

    //2 , 通过socket生成数据 nc -lp 7777
    val stream = env.socketTextStream("localhost",7777)
    val dataStream = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    })

      //常用的写法
      .assignTimestampsAndWatermarks(
        // 延迟时间 1s
        new BoundedOutOfOrdernessTimestampExtractor[SensorReading]( Time.seconds(1)) {
          override def extractTimestamp(t: SensorReading): Long = {
            t.timestamp * 1000
          }
        })
    //处理温度1s钟连续上升
    val processedStream = dataStream
      .process(new FreezingAlert())

//    dataStream.print("input data")
    //processedStream.print("TempIncrAlert data")//主输出流
    //读取侧输出流
    processedStream.getSideOutput(new OutputTag[String]("freezing alert")).print("freezing alert")

    env.execute("window test")
  }
}

//冰点报警，如果小于32F,输出报警信息到侧输出流
class FreezingAlert extends ProcessFunction[SensorReading,SensorReading]{
  lazy  val alertOutput:OutputTag[String] = new OutputTag[String]("freezing alert")
  override def processElement(value: SensorReading,
                              ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                              out: Collector[SensorReading]): Unit = {
    if ( value.temperature < 32.0){
      ctx.output(alertOutput,"freezing alert for "+value.id) //侧输出流
    }else{
      out.collect(value)
    }
  }
}