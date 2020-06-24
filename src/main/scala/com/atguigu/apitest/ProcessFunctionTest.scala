package com.atguigu.apitest

import com.atguigu.apitest.SourceTest.getClass
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
//自定义处理函数
object ProcessFunctionTest {
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
    val processedStream = dataStream.keyBy(_.id)
        .process(new TempIncrAlert())

    dataStream.print("input data")
    processedStream.print("TempIncrAlert data")

    env.execute("window test")
  }
}

//处理温度1s钟连续上升
//状态编程
class TempIncrAlert extends KeyedProcessFunction[String,SensorReading,String]{

  //定义一个状态，用来保存上一个数据的温度值
  lazy val lastTemp :ValueState[Double] = getRuntimeContext().getState(new ValueStateDescriptor[Double]("last-temp",classOf[Double]))
  lazy  val currentTimer:ValueState[Long]= getRuntimeContext().getState(new ValueStateDescriptor[Long]("current-timer",classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    //先取出上一个温度值
    val preTemp = lastTemp.value()
    //更新温度值
    lastTemp.update(value.temperature)
    val curTimerTs = currentTimer.value()
    //温度上升而且没设置定时器,则注册定时器
    if(value.temperature > preTemp && curTimerTs == 0){
      //当前处理时间+1s
      val timeTs = ctx.timerService().currentProcessingTime() + 1000L
      ctx.timerService().registerProcessingTimeTimer(timeTs)
      currentTimer.update(timeTs)
    }else if ( preTemp > value.temperature  || preTemp == 0.0){ //温度下降
      //如果温度下降，或者第一条数据，删除定时器并清空状态
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      currentTimer.clear()//清空状态
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    //输出报警信息
    out.collect( ctx.getCurrentKey + " 温度连续上升" )
    currentTimer.clear()
  }
}
