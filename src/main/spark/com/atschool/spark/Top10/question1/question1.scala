package com.atschool.spark.Top10

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

/**
 * 倒数第二个是支付的产品ID
 * 倒数第三个是支付的品类ID
 * 4 ==> 下单的产品ID
 * 5 ==> 下单的品类ID
 * 6 ==> 点击产品ID
 * 7 ==> 点击品类ID
 * 8 ==> 点击的内容
 *
 * @author Bonze
 * @create 2020-11-09-15:52
 */

class question1 {
  @Test
  def test(){
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("input/user_visit_action.txt", 2)
    val splitedLines: RDD[Array[String]] = lines.map(_.split("_"))

    //产品Id拼1, 点击数
    var array = new ArrayBuffer[Tuple2[String, Int]]
    var array1 = new ArrayBuffer[Tuple2[String, Int]]
    var array2 = new ArrayBuffer[Tuple2[String, Int]]

    splitedLines.collect().foreach(k => {
      val strings: Array[String] = k(k.length - 7).split(",")
      for (elem <- strings) {
        array += (elem -> 1)
      }
    })

    val clickValue: RDD[(String, Int)] = sc.makeRDD(array.toArray)

    splitedLines.collect().foreach(k => {
      val strings: Array[String] = k(k.length - 5).split(",")
      for (elem <- strings) {
        array1 += (elem -> 1)
      }
    })

    val buyValue: RDD[(String, Int)] = sc.makeRDD(array1.toArray)

    splitedLines.collect().foreach(k => {
      val strings: Array[String] = k(k.length - 3).split(",")
      for (elem <- strings) {
        array2 += (elem -> 1)
      }
    })

    val payValue: RDD[(String, Int)] = sc.makeRDD(array2.toArray)

    val clickResult: RDD[(String, Int)] = clickValue.reduceByKey(_ + _)
    val buyResult: RDD[(String, Int)] = buyValue.reduceByKey(_ + _)

    val payResult: RDD[(String, Int)] = payValue.reduceByKey(_ + _)

    val value: RDD[(String, (Iterable[Int], Iterable[Int]))] = clickResult.cogroup(buyResult)

    var temp: Int = 0
    var temp1: Int = 0
    var temp2: Int = 0

    val value2: Array[(String, (Int, Int))] = value.collect().map(k => {
      for (elem <- k._2._1) {
        temp = elem
      }

      for (elem <- k._2._2) {
        temp1 = elem
      }

      (k._1, (temp, temp1))
    })


    val value5: RDD[(String, (Int, Int))] = sc.makeRDD(value2)
    val value1: RDD[(String, (Iterable[(Int, Int)], Iterable[Int]))] = value5.cogroup(payResult)

    val tuples: Array[(String, (Int, Int, Int))] = value1.collect().map(k => {
      for (elem <- k._2._1) {
        temp = elem._1
        temp1 = elem._2
      }
      for (elem <- k._2._2) {
        temp2 = elem
      }
      (k._1, (temp, temp1, temp2))
    })
    val value3: RDD[(String, (Int, Int, Int))] = sc.makeRDD(tuples)

    val value4: RDD[(String, (Int, Int, Int))] = value3.sortBy(k => {
      (-k._2._1, -k._2._2, -k._2._3)
    })

    val tuples1: Array[(String, (Int, Int, Int))] = value4.take(10)

    val top10: RDD[(String, (Int, Int, Int))] = sc.makeRDD(tuples1)




//    tuples1.foreach(
//      k => {
//        if (k._1 != "null" && k._1 != "-1") {
//          print("产品Id" + k._1)
//          print("点击量" + k._2._1)
//          print("下单量" + k._2._2)
//          println("支付量" + k._2._3)
//        }
//      }
//    )
  }
  @Test
  def question12(): Unit ={

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("input/user_visit_action.txt", 2)
    val splitedLines: RDD[Array[String]] = lines.map(_.split("_"))


    val acc: user_visit_Acc = new user_visit_Acc

    splitedLines.collect().foreach(k => {
      val strings1: Array[String] = k(k.length - 7).split(",")
      for (elem <- strings1) {
        if (elem != "-1") acc.add((elem, (1, 0, 0)))
      }
      val strings2: Array[String] = k(k.length - 5).split(",")
      for (elem <- strings2) {
        if (elem != "-1") acc.add((elem, (0, 1, 0)))
      }
      val strings3: Array[String] = k(k.length - 3).split(",")
      for (elem <- strings3) {
        if (elem != "-1") acc.add((elem, (0, 0, 1)))
      }
    })

    val value: RDD[(String, (Int, Int, Int))] = sc.makeRDD(acc.value.toArray)
    val value1: RDD[(String, (Int, Int, Int))] = value.sortBy(k => {
      (-k._2._1, -k._2._2, -k._2._3)
    })


    value1.collect().foreach(k => {
      if(!k._1.equals("null")) println("商品Id" + k._1 + "点击量" + k._2._1 + "订单量" + k._2._2 + "支付量" + k._2._3)
    })

  }

}

class user_visit_Acc() extends AccumulatorV2 [(String, (Int, Int, Int)), Map[String, (Int, Int, Int)]]{

  private var map: Map[String, (Int, Int, Int)] = Map()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[(String, (Int, Int, Int)), Map[String, (Int, Int, Int)]] = new user_visit_Acc

  override def reset(): Unit = Map()

  override def add(v: (String, (Int, Int, Int))): Unit = {

    var temp1 = v._2._1 + map.getOrElse(v._1, (0, 0, 0))._1
    var temp2 = v._2._2 + map.getOrElse(v._1, (0, 0, 0))._2
    var temp3 = v._2._3 + map.getOrElse(v._1, (0, 0, 0))._3

    map += (v._1 -> (temp1, temp2, temp3) )
  }


  override def merge(other: AccumulatorV2[(String, (Int, Int, Int)), Map[String, (Int, Int, Int)]]): Unit = {
    var map1 = map
    var map2 = other.value

    map = map1 ++ map2.map(t => {

      val temp1: Int = t._2._1 + map1.getOrElse(t._1, (0, 0, 0))._1
      val temp2: Int = t._2._2 + map1.getOrElse(t._1, (0, 0, 0))._2
      val temp3: Int = map1.getOrElse(t._1, (0, 0, 0))._3

      (t._1, (temp1, temp2, temp3))

    })

  }

  override def value: Map[String, (Int, Int, Int)] = map
}//用户访问动作表

case class UserVisitAction(
                            date: String, //用户点击行为的日期
                            user_id: Long, //用户的ID
                            session_id: String, //Session的ID
                            page_id: Long, //某个页面的ID
                            action_time: String, //动作的时间点
                            search_keyword: String, //用户搜索的关键词
                            click_category_id: Long, //某一个商品品类的ID
                            click_product_id: Long, //某一个商品的ID
                            order_category_ids: String, //一次订单中所有品类的ID集合
                            order_product_ids: String, //一次订单中所有商品的ID集合
                            pay_category_ids: String, //一次支付中所有品类的ID集合
                            pay_product_ids: String, //一次支付中所有商品的ID集合
                            city_id: Long
                          ) //城市 id