package com.atguigu.apitest

import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object UdfTest {
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
    //自定义类
    stream1.filter(new MyFilter()).print("实现接口类").setParallelism(1)
    //匿名类
    stream1.filter(new FilterFunction[SensorReading] {
      override def filter(t: SensorReading): Boolean = {
        t.id.startsWith("sensor_1")
      }
    }).print("匿名类").setParallelism(1)

    //匿名函数
    stream1.filter(data => data.id.startsWith("sensor_1")).print("匿名函数1").setParallelism(1)
    //等价于
    stream1.filter(_.id.startsWith("sensor_1")).print("匿名函数2").setParallelism(1)
    env.execute("udf test")
  }
}
class MyFilter extends FilterFunction[SensorReading]{
  override def filter(t: SensorReading): Boolean = {
    t.id.startsWith("sensor_1")
  }
}

class MyMapper() extends RichMapFunction[SensorReading,String]{
  var subTaskIndex = 0
  //open()方法是 rich function 的初始化方法，当一个算子例如 map 或者 filter
  //被调用之前 open()会被调用。
  override def open(parameters: Configuration): Unit = {
    subTaskIndex = getRuntimeContext.getIndexOfThisSubtask
    // 以下可以做一些初始化工作，例如建立一个和 HDFS 的连接
  }

  //close()方法是生命周期中的最后一个调用的方法，做一些清理工作。
  override def close(): Unit = {
    // 以下做一些清理工作，例如断开和 HDFS 的连接。
  }

  override def map(in: SensorReading): String = {
    "flink"
  }
}