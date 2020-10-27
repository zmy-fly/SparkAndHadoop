package test

import scala.beans.BeanProperty
import scala.io.StdIn

/**
 * @author Bonze
 * @create 2020-09-11-11:32
 */
object Exer {
  def main(args: Array[String]): Unit = {

//    implicit def Transfer(int : Int): Double = {
//      int.toDouble
//    }
//
//    implicit def Transfer1(circle: Circle) : Dog = {
//      new Dog
//    }

//    implicit val name: String = "zmy"
//    print(name:String)
//    var circle : Circle = new Circle
//    circle.cry()

    val ints = Array(0, 1, 2, 3)
    val ints1 = ints.map(_ * 10)
    for (elem <- ints1) {
      print(elem)
    }
    var oper: String = StdIn.readLine()
    var n1: Int = 2
    var n2: Int = 3
    var res = 0
    oper match {
      case "+" => res = n1 + n2
      case "-" => res = n1 - n2
      case "*" => res = n1 * n2
      case "/" => res = n1 / n2
      case  _  => println("wrong input")
    }


  }
}


//class Dog{
//  var name: String = _
//  var age: Int = _
//  def cry(): String ={
//    "汪汪汪"
//  }
//}


class Circle{
  @BeanProperty
  var radius:Double = _
  def findArea(): Double ={
    3.14 * radius * radius
  }
}

class Cylinder extends Circle{
  @BeanProperty
  var length:Double = _
  
}