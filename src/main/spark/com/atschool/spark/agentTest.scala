package com.atschool.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

/**
 * @author Bonze
 * @create 2020-10-21-9:46
 */
class agentTest {
  @Test
  def test(): Unit ={
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark")
    val context = new SparkContext(conf)
    val value: RDD[String] = context.textFile("input/agent.log")
    val value1: RDD[Array[String]] = value.map(_.split(" "))

    val value6: RDD[(Array[String], Int)] = value1.map((_, 1))

    val value2: RDD[(String, Iterable[(Array[String], Int)])] = value6.groupBy(_._1(1))
      .
    val value3: RDD[(String, Array[(String, Int)])] = value2.map(k => {
      val grouped: Map[String, Array[(Array[String], Int)]] = k._2.toArray.groupBy(v => {
        v._1(4)
      })

      val adCounts: Map[String, Int] = grouped.map(k => {
        (k._1, k._2.length)
      })

      val tuples: Array[(String, Int)] = adCounts.toArray.sortBy(-_._2)
      (k._1, tuples)
    })
    value3.collect().foreach(k => {
      println("省份：" + k._1)
      var count: Int = 0;
      val tuples: Array[(String, Int)] = k._2.take(3)
      tuples.foreach(v => {
        println("广告名字：" + v._1 + "用户访问量" + v._2)
      })
    }
    )
  }


}
