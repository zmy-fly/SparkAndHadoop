package com.school.scala

import org.junit.Test

import scala.beans.BeanProperty
/**
 * @author Bonze
 * @create 2020-09-03-15:57
 */
class Hello {
  @Test
  def test(): Unit ={
    var lists = Array(1,2,3,4,5)
//    lists.map(x => x*x).foreach(println)
//    lists.map((x:Int) => {
//    })
//    lists.filter(x => x % 2 == 0).map(x => x * x).foreach(println)

    lists.map(_*10).foreach(println)
    var f = (x:Int) => x % 2 == 0

  }


  def f1(x:Int) :Int = {
    1
  }

  @Test
  def test1(): Unit ={
  }


  def main(args: Array[String]): Unit = {
    //    println(Febo(5))
    //    println(sum(1, 2, 3, 4, 5))
    //    make(4)


    //    var cat = new Cat
    //    cat.name = "zrh"
    //    cat.age = 20
    //    cat.color = "white"
    //    println(cat.toString)
    var rect = new MethodExec
    rect.PrintRectangle()
    var consumer = new Consumer
  //    println(consumer.name)
    var ints = Array(1, 2, 3)

    val arr1 = ints.filter(i => i % 2 == 0).map(i => i * 100)

    var n = 1
    var n2 = 2
//    println(sum(n, n2))
//    for (elem <- arr1) {
//      print(elem)
//    }

    var f: Int => Boolean = (x:Int) =>{
      x % 2 == 0
    }
    var lists = List(1,2,3,4,"abc")



  }




  //
  //  def make(n:Int): Unit = {
  //    for(i <- 1 to n) {
  //
  //      for(j <- 1 to n-i){
  //        print(" ")
  //      }
  //
  //      for(j <- 1 to 2*i-1){
  //        print("*")
  //      }
  //
  //      for(j <- 1 to n-i){
  //        print(" ")
  //      }
  //      println()
  //    }
  //  }

  //  def sum(n1:Int, args:Int*):Int ={
  //   var all = n1
  //   for(temp <- args){
  //    all += temp
  //    }
  //  all
  //  }
  //  def Febo(n: Int): Int = {
  //    if (n == 1 || n == 2) 1
  //    else Febo(n - 2) + Febo(n - 1)
  //  }qwe
  //
  //}
}

class Cat {
  var name: String = ""
  var age: Int = 0
  var color: String = ""

  def eat(): Unit = {
    println("Cat is eating")
  }

  override def toString = s"Cat($name, $age, $color)"
}

class MethodExec {

  def PrintRectangle(): Unit = {
    for (i <- 1 to 10) {
      for (j <- 1 to 8) {
        print("*")
      }
      println()
    }
  }
}

class Dog {
  var name: String = _
  var age: Int = _
  var weight: Double = _

  def say(): String = s"($name,$age,$weight)"

}

class Box {
  var length: Double = _
  var width: Double = _
  var height: Double = _

  def getVolumn(): Double = {
    length * width * height
  }
}

class Consumer(){
  @BeanProperty val name: String = ""
  @BeanProperty var age : String = ""
}


