package com.atguigu.apitest

import com.atguigu.apitest.SourceTest.getClass
import org.apache.flink.streaming.api.scala._
//转换算子
object TransformTest {
  def main(args: Array[String]): Unit = {
    //1，获取环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //统一设置的并行度，不影响结果正确性
      env.setParallelism(1)
    //2，从自定义的集合中读取数据
    val stream1 = env.fromCollection(List(
      SensorReading("sensor_1",1547718199,35.80018),
      SensorReading("sensor_6",1547718201,15.40018),
      SensorReading("sensor_7",1547718202,6.72018),
      SensorReading("sensor_10",1547718205,38.80018)
    ))

    stream1.map(x=>{
      //不能直接修改x的值
      //x.temperature += 1.0
      //x

      //这样子的修改
      val t0 = x.temperature + 1.0
      SensorReading(x.id,x.timestamp,t0)
    }).print("stream1").setParallelism(1)


    //2
    val stream2 = env.readTextFile(getClass.getResource("/sensor.txt").getPath)
    //
    val dataStream:DataStream[SensorReading] = stream2.map(data => {
      //按逗号分隔
      val dataArray = data.split(",")
      //返回SensorReading
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    })

    dataStream
      //.keyBy(0)// 按id分组，得到KeyedStream
        .keyBy(_.id)//常用
      .sum(2)//聚合温度值
      .print("stream2").setParallelism(1)


    env.execute("transform test")
  }
}
