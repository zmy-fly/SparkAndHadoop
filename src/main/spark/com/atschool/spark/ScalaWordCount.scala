package com.atschool.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Bonze
 * @create 2020-09-14-16:23
 */
object ScalaWordCount {
  def main(args: Array[String]): Unit = {
    if(args.length!=2){
      println("Usage:com.Demo1.Text1 <input><output>")
      sys.exit(1)
    }
    //接收参数
    val Array(input,output) = args
    //创建一个配置对象
    val conf:SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(ScalaWordCount.getClass.getSimpleName)
    //创建一个SparkContext
    val sc:SparkContext = new SparkContext(conf)
    //读取文件
    val files: RDD[String] = sc.textFile(input)
    //切分并压平
    val splitedData: RDD[String] = files.flatMap(_.split(" "))
    //组装
    val wordsAndOne: RDD[(String, Int)] = splitedData.map((_,1))
    //进行分组聚合
    val result: RDD[(String, Int)] = wordsAndOne.reduceByKey(_+_)
    //排序
    val sorted: RDD[(String, Int)] = result.sortBy(-_._2)
    //存储结果数据
    sorted.saveAsTextFile(output)
    //释放资源
    sc.stop()
    //spark中RDD中的方法名可能和scala中一致。
    //spark中调用的是RDD中的flatMap方法，scala中的flatMap是本地集合的方法。
  }
}
