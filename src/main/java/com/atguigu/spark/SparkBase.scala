package com.atguigu.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test


/**
 * @author Bonze
 * @create 2020-09-24-15:42
 */
class SparkBase {
  @Test
  def test(): Unit ={
   var sparkConf =
     new SparkConf().setMaster("local[*]").setAppName("spark")

    val sparkContext = new SparkContext(sparkConf)

    var rdd1 = sparkContext.parallelize(
      List(1,2,3,4)
    )
    val rdd2 = sparkContext.makeRDD(
      List(1, 2, 3, 4)
    )
    rdd1.collect().foreach(println)
    rdd2.collect().foreach(println)
    sparkContext.stop()


  }

  @Test
  def test1(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)
    val fileRDD: RDD[String] = sparkContext.textFile("input")
    fileRDD.collect().foreach(println)
    sparkContext.stop()
  }

  @Test
  def test2(): Unit ={
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)
    val dataRDD:RDD[Int] = sparkContext.makeRDD(List(1,2,3,4),4)
    val fileRDD:RDD[String] = sparkContext.textFile("input",2)
    fileRDD.collect().foreach(println)
    sparkContext.stop()
  }


}
