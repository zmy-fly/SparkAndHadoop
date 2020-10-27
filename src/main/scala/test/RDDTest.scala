package test

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

/**
 * @author Bonze
 * @create 2020-09-28-16:42
 */
class RDDTest {
  @Test
  def test(): Unit = {

    val sparkConf =
      new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)
    val rdd1 = sparkContext.parallelize(
      List(1, 2, 3, 4)
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
    val context = new SparkContext(sparkConf)
    val fileRDD: RDD[String] = context.textFile("input")
    fileRDD.collect().foreach(println)
    context.stop()
  }

  @Test
  def test2(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val context = new SparkContext(sparkConf)
    val dataRDD: RDD[Int] = context.makeRDD(
      List(1, 2, 3, 4), 4
    )
    val fileRDD: RDD[String] = context.textFile(
      "input", 2
    )
    fileRDD.collect().foreach(println)
    context.stop()
  }

  @Test
  def test3(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")

    val context = new SparkContext(sparkConf)
    val dataRDD: RDD[Int] = context.makeRDD(List(1, 2, 3, 4))
    val dataRDD1: RDD[Int] = dataRDD.map(num => num * 2)
    dataRDD1.collect().foreach(println)
  }

  @Test
  def test4(): Unit = {
    var sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)
    val fileRDD: RDD[String] = sparkContext.textFile("input/score.txt") //文件或者目录都可以

    //    fileRDD.map(x => x.split(",")).collect().foreach(println)

    // 文件还可以指向第三方存储路径：Hdfs

    //扁平化：flatMap
    val value: RDD[List[Int]] = sparkContext.makeRDD(Array(List(1, 2, 3), List(4, 5, 6)))
    val value1: RDD[Int] = value.flatMap(x => x)
    value1.collect().foreach(println)


    //    fileRDD.groupBy(fileRDD)
  }

  @Test
  def test5(): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)
    val lines: RDD[String] = sparkContext.textFile("input/score.txt")

    val linesStr: Array[String] = lines.collect()

    val value: RDD[Array[String]] = sparkContext.makeRDD(linesStr).map(
      _.split(",")
    )



    var listStr1 : ArrayBuffer[String] = new ArrayBuffer[String]()
    var listStr : ArrayBuffer[String] = new ArrayBuffer[String]()
    //


    value.foreach(line => {
      var sum: Double = 0
      var count = line.length - 2
      for (i <- 2 to line.length - 1) {
        val score: String = line(i)
        sum += score.toDouble
      }
      var aver = sum / count
      var averStr = String.valueOf(aver)
      val perScore: String = line(0) + "," + line(1) + "," + averStr
      listStr1 += (perScore)
    }
    )
    listStr1.foreach(println)
    //   computer huangzitao 87.0
    //    // [[ [77] [computer]], [[88] [computer]]]

    val splited: ArrayBuffer[Array[String]] = listStr.map(
      x => {
        x.split(",")
      }
    )


    val allRDD: RDD[Array[String]] = sparkContext.makeRDD(splited)
    // computer [ computer huangzitao 87.0 ]
    //          [ computer askldjklas 125.0]


    val groupedRDD: RDD[(String, Iterable[Array[String]])] = allRDD.groupBy(x => {
      x(0)
    }
    )
    val grouped: Array[(String, Iterable[Array[String]])] = groupedRDD.collect()

    println("~~~~~")
    grouped.foreach(println)
    groupedRDD.foreach(
      group => {
        println("课程:" + group._1)
        var count = 0
        var sum: Double = 0
        // elems Array[String]
        for (elems <- group._2) {
          count = count + 1
          sum += elems(2).toDouble
        }
        println("课程总人数:" + count + "课程平均分:" + sum / count)
      }
    )


    //    allRDD.groupBy(
    //      x => {
    //        x(1)
    //      }
    //    )


    //    val result: RDD[String] = lines.map(line => {
    //      var sum: Double = 0
    //      var count = line.length - 2
    //      for (i <- 2 to line.length - 1) {
    //        val score: Char = line.charAt(i)
    //        sum += score.asInstanceOf[Double]
    //      }
    //      var aver = sum / count
    //      String.valueOf(aver)
    //    }
    //    )


    //    val data: Array[Array[String]] = line.collect().map(
    //      s => s.split(",")
    //    )


    //    data.foreach(s => {
    //      k
    //    })


  }
  //第一题
  @Test
  def test6(): Unit ={
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)
    val lines: RDD[String] = sparkContext.textFile("input/score.txt",1)

    val splitedRDD: RDD[Array[String]] = lines.map(_.split(","))

    val result: RDD[Array[Any]] = splitedRDD.map(
      x => {
        var sum: Double = 0
        var count = x.length - 2
        var aver: Double = 0
        for (i <- 2 to x.length - 1) {
          sum += x(i).toDouble
        }
        aver = sum / count
        Array(x(0), x(1), aver)
      }
    )
    val value: RDD[(Any, Iterable[Array[Any]])] = result.groupBy(
      _(0)
    )

    value.foreach(
      x => {
        println("课程名:" + x._1)
        var count = 0
        for (elem <- x._2) {
          count += 1
          print("姓名:" + elem(1) + "--")
          println("score:" + elem(2).formatted("%.1f"))
        }
        println("课程总人数" + count)
      }
    )
  }
  // 第二题

  @Test
  def test7(): Unit ={
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)
    val lines: RDD[String] = sparkContext.textFile("input/score.txt",1)

    val splitedRDD: RDD[Array[String]] = lines.map(_.split(","))

    val result: RDD[(String, String, Double)] = splitedRDD.map(
      x => {
        var sum: Double = 0
        var count = x.length - 2
        var aver: Double = 0
        for (i <- 2 to x.length - 1) {
          sum += x(i).toDouble
        }
        aver = sum / count

        (x(0), x(1), aver.formatted("%.1f").toDouble)
      }
    )
    // 降序
    val value: RDD[(String, String, Double)] = result.sortBy(x => -x._3)

    //变成[course, [name,score] ]
    val value2: RDD[(String, Array[Any])] = value.map(x => {
      (x._1, Array(x._2, x._3))
    })
//    val value1: RDD[(String, Array[Any])] = value2.repartition(4)
//    分区
    val value1: RDD[(String, Array[Any])] = value2.partitionBy(new MyPartitioner(4))

    val value3: RDD[(String, String, Double)] = value1.map(x => {
      (x._1, x._2(0).toString, x._2(1).toString.toDouble)
    })
//    val value4: RDD[(String, Iterable[(String, Array[Any])])] = value1.groupBy(_._1)

    val conf: Configuration = sparkContext.hadoopConfiguration
    val fs: FileSystem = FileSystem.get(conf)
    val path = new Path("output")

    if(fs.exists(path)){
      fs.delete(path,true)
    }

    value3.saveAsTextFile("output")
  }

  //第三题
  @Test
  def test8(): Unit ={
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)
    val lines: RDD[String] = sparkContext.textFile("input/score.txt",1)

    val splitedRDD: RDD[Array[String]] = lines.map(_.split(","))

    val result: RDD[(String, String, Double)] = splitedRDD.map(
      x => {
        var sum: Double = 0
        var count = x.length - 2
        var aver: Double = 0
        for (i <- 2 to x.length - 1) {
          sum += x(i).toDouble
        }
        aver = sum / count
        (x(0), x(1), aver)
      }
    )
    val value: RDD[(String, String, Double)] = result.sortBy(x => -x._3)

    val value1: RDD[(String, Iterable[(String, String, Double)])] = value.groupBy(
      _._1
    )

//    val value3: RDD[(String, Array[(String, String, Double)])] = value1.map(x => {
//      val value2: RDD[(String, String, Double)] = sparkContext.makeRDD(x._2.toArray)
//      val tuples: Array[(String, String, Double)] = value2.take(1)
//      (x._1, tuples)
//    })


    value1.foreach(
      x => {
        println("课程名:" + x._1)
        var flag : Boolean = true
        for (elem <- x._2 if flag) {
          print("姓名:" + elem._2 + "--")
          println("score:" + elem._3.formatted("%.1f"))
          flag = false
        }
      }
    )

  }

  @Test
  def test9(): Unit ={
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark")
    val sc = new SparkContext(sparkConf)
    val value: RDD[String] = sc.textFile("input/score.txt", 1)
    val value1: RDD[String] = value.flatMap(x => x.split(","))


  }



}
class MyPartitioner(numpart: Int) extends Partitioner{
  override def numPartitions: Int = numpart

  override def getPartition(key: Any): Int = {
    if(key.equals("computer") ) 0
    else if(key.equals("math") ) 1
    else if(key.equals("english") ) 2
    else if(key.equals("algorithm") ) 3
    else 0
  }
}