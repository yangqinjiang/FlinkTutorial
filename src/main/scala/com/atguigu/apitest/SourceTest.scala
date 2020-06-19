package com.atguigu.apitest

import org.apache.flink.streaming.api.scala._

//温度传感器读数样例类=
case class SensorReading(id:String, timestamp:Long,temperature:Double)
object SourceTest {

  def main(args: Array[String]): Unit = {
    //1，获取环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //2，从自定义的集合中读取数据
    val stream1 = env.fromCollection(List(
      SensorReading("sensor_1",1547718199,35.80018),
      SensorReading("sensor_6",1547718201,15.40018),
      SensorReading("sensor_7",1547718202,6.72018),
      SensorReading("sensor_10",1547718205,38.80018)
    ))
    stream1.print("stream1").setParallelism(1)

    //3，从文件中读取数据
    val stream2 = env.readTextFile(getClass.getResource("/sensor.txt").getPath)
    stream2.print("stream2").setParallelism(1)

    //4,
    env.fromElements(1,2,3.0,"string").print("fromElements").setParallelism(1)

    env.execute("source test")
  }
}
