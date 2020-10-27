package com.atschool.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

/**
 * @author Bonze
 * @create 2020-10-12-16:55
 */
class ScalaDemo {
  @Test
  def test(): Unit ={
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)
    var arr = sparkContext.textFile("input/Array.txt",2)

    var conf : Configuration = new Configuration()
    var fs : FileSystem = FileSystem.get(conf)
    var path : Path = new Path("outputTest")

    if(fs.exists(path)){
      fs.delete(path, true)
    }
    arr.saveAsTextFile("outputTest")
    sparkContext.stop()
  }


  @Test
  def test1(): Unit ={
    val spark: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark")
    val sc = new SparkContext(spark)
    val ints: Array[Int] = Array(1, 6, 34, 1, 12, 3, 4, 5)
    val value: RDD[Int] = sc.makeRDD(ints)


  }

  @Test
  def test2(): Unit ={
    val spark: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark")
    val sc = new SparkContext(spark)

  }


}
