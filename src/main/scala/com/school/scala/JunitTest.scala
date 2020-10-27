package com.school.scala

import org.junit.Test

/**
 * @author Bonze
 * @create 2020-09-17-21:44
 */
class JunitTest {
  @Test
  def hel():Unit = {
    var some = "s"
    println(some)
  }
  @Test
  def Test1():Unit = {
    val list = List(1, 2, 3, 4, "abc")
    val ints = list.filter(n => n.isInstanceOf[Int]).map(n => n.asInstanceOf[Int]).map(_ + 1)
    for (elem <- ints) {
      println(elem)
    }
  }

  @Test
  def test2():Unit = {

  }


}
