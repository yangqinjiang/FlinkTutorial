package com.atguigu.wc

import org.apache.flink.api.scala._

//批处理wordcount
object WordCount {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 从文件中读取数据，相对路径
    val inputPath = getClass.getResource("/hello.txt").getPath
    val inputDS: DataSet[String] = env.readTextFile(inputPath)
    // 分词之后，对单词进行 groupby 分组，然后用 sum 进行聚合
    val wordCountDS: AggregateDataSet[(String, Int)] = inputDS.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)
    // 打印输出
    wordCountDS.print()
  } }
