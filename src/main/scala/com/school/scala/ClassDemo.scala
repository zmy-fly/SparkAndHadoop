package com.school.scala

import org.junit.Test

import scala.io.Source

/**
 * @author Bonze
 * @create 2020-10-20-23:52
 */
class ClassDemo {
  @Test
  def test(): Unit ={

    val array: Array[String] = Source.fromFile("input/score.txt").getLines().toArray
    val splited: Array[Array[String]] = array.map(_.split(","))


    val tuples: Array[(String, String, Double)] = splited.map(k => {
      var sum: Double = 0
      for (i <- 2 until k.length) {
        sum += k(i).toDouble
      }
      var count: Int = k.length - 2
      (k(0), k(1), sum / count)
    })


    val groupedByCourse: Map[String, Array[(String, String, Double)]] = tuples.groupBy(_._1)


    groupedByCourse.foreach(k => {
      println("课程名为: " + k._1)
      println("人数为: " + k._2.length)
      k._2.foreach(v => {
        println(v._2 + " " + v._3)
      })
    })


  }
  @Test
  def test1(): Unit = {
    val array: Array[String] = Source.fromFile("input/score.txt").getLines().toArray
    val splited: Array[Array[String]] = array.map(_.split(","))


    val tuples: Array[(String, String, Double)] = splited.map(k => {
      var sum: Double = 0
      for (i <- 2 until k.length) {
        sum += k(i).toDouble
      }
      var count: Int = k.length - 2
      (k(0), k(1), sum / count)
    })

    val sorted: Array[(String, String, Double)] = tuples.sortBy(-_._3)
    val groupedByCourse: Map[String, Array[(String, String, Double)]] = sorted.groupBy(_._1)

    groupedByCourse.foreach(k => {
      println("课程名为: " + k._1)
      println("人数为: " + k._2.length)
      k._2.foreach(v => {
        println(v._2 + " " + v._3)
      })
    })

  }
  @Test
  def test2(): Unit ={
    val array: Array[String] = Source.fromFile("input/score.txt").getLines().toArray
    val splited: Array[Array[String]] = array.map(_.split(","))


    val tuples: Array[(String, String, Double)] = splited.map(k => {
      var sum: Double = 0
      for (i <- 2 until k.length) {
        sum += k(i).toDouble
      }
      var count: Int = k.length - 2
      (k(0), k(1), sum / count)
    })

    val sorted: Array[(String, String, Double)] = tuples.sortBy(-_._3)
    val groupedByCourse: Map[String, Array[(String, String, Double)]] = sorted.groupBy(_._1)

    groupedByCourse.foreach(k => {
      println("课程名为: " + k._1)
      println(k._2(0)._2 + k._2(0)._3)
    })

  }


}
