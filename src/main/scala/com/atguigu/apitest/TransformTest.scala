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

    //keyBy: 逻辑地将一个流拆分成不相交的分区，每个分区包含具有相同 key 的元素，在内部以 hash 的形式实现的
    dataStream
      //.keyBy(0)// 按id分组，得到KeyedStream
        .keyBy(_.id)//常用
      //.sum(2)//需求1：聚合温度值
      //需求2：输出当前传感器最新的温度+10,而时间戳是上一次数据的时间+1
      .reduce((x,y)=> SensorReading(x.id, x.timestamp+1, y.temperature+10))
      .print("stream2").setParallelism(1)


    //3 多流转换算子
    //split方式的分流
    val stream3 = env.readTextFile(getClass.getResource("/sensor.txt").getPath)
    //
    val dataStream3:DataStream[SensorReading] = stream3.map(data => {
      //按逗号分隔
      val dataArray = data.split(",")
      //返回SensorReading
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    })

    //keyBy: 逻辑地将一个流拆分成不相交的分区，每个分区包含具有相同 key 的元素，在内部以 hash 的形式实现的
    dataStream3
      //.keyBy(0)// 按id分组，得到KeyedStream
      .keyBy(_.id)//常用

    val splitStream = dataStream3.split(data => {
      //判断，输出不同的流
      if (data.temperature > 30) Seq("high") else Seq("low")
    })
    //从一个 SplitStream 中获取一个或者多个 DataStream
    val high = splitStream.select("high")
    val low = splitStream.select("low")
    val all = splitStream.select("high","low")
    high.print("high")
    low.print("low")
    all.print("all")

    //合并两条流,而且只能是两条流，如果是多条流，则使用Union算子
    //数据结构可以不一致
    val warning = high.map(data => (data.id,data.temperature))
    val connectedStream = warning.connect(low)
    //map 接收两个函数
    val coMapDataStream = connectedStream.map(
      //元组，返回值也是元组
      warningData => (warningData._1,warningData._2,"warning"),
      //Sensor数据，返回值是元组
      lowData =>(lowData.id,lowData.temperature,"healthy")
    )
    coMapDataStream.print("coMapDataStream")

    //Union算子, 接收的数据结构必须一致
    val unionStream = high.union(low)//多条流，其数据结构必须一致
    unionStream.print("union")

    env.execute("transform test")
  }
}

/**
 * Connect 与 Union 区别：
 * 1． Union 之前两个流的类型必须是一样，Connect 可以不一样，在之后的 coMap
 * 中再去调整成为一样的。
 * 2. Connect 只能操作两个流，Union 可以操作多个。
 */
