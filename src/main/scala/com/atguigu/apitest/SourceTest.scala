package com.atguigu.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

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

    //5，从kafka源读取数据
    //配置
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","localhost:9092")
    properties.setProperty("group.id","consumer-group")

    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    //运行出错时，flink将偏移量状态恢复到kafka
    //自动保证运行
    val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(),properties))
    stream3.print("source kafka steam3").setParallelism(1)


    // 6 自定义source
    val stream4 = env.addSource(new MySensorSource())
    stream4.print("MySensorSource").setParallelism(1)
    env.execute("source test")
  }
}
//自定义source,随机生成数据
class MySensorSource extends  SourceFunction[SensorReading]{

  //标识变量，表示数据源是否还在正常运行
  var running:Boolean = true
  override def cancel(): Unit = {
    running = false // 取消时， 设置为false
  }

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    //初始化一个随机数发生器
    val rand = new Random()

    var curTemp = 1.to(10).map(
      i => ("sensor_"+i , 65 + rand.nextGaussian() * 20)
    )

    while (running){
      //更新温度值
      curTemp = curTemp.map(
        t => (t._1, t._2 + rand.nextGaussian())
      )

      //获取当前时间戳
      var curTime = System.currentTimeMillis()

      curTemp.foreach(
        t => ctx.collect(SensorReading(t._1,curTime,t._2))
      )
      Thread.sleep(1000)//休眠x ms
    }
  }



}