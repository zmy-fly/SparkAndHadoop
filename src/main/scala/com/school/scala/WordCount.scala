package com.school.scala

import org.junit.Test

/**
 * @author Bonze
 * @create 2020-10-22-15:50
 */
class WordCount {
  @Test
  def test(): Unit ={
    val lines: Array[String] = Array("Hello World", "Hello Java", "Hello Scala")

    val stringses: Array[Array[String]] = lines.map(_.split(" "))

    val flatten: Array[String] = stringses.flatten



    val stringToInt: Map[String, Int] = flatten.groupBy(t => t).map(k => {
      (k._1, k._2.length)
    })

    stringToInt.foreach(k => {
      println(k._1 + "个数为" + k._2)
    })

  }



}
