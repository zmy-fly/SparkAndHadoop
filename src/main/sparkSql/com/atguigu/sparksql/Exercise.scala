package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, TypedColumn}
import org.apache.spark
import org.junit.Test

/**
 * @author Bonze
 * @create 2020-10-15-9:55
 */
class Exercise {
  @Test
  def test(): Unit = {
    var sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val frame: DataFrame = spark.read.json("input/test.json")
    import spark.implicits._

    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 17), (2, "lisi", 18)))


    /**
     * 第一种方法， 封装成外部类
     */
    //    val df: DataFrame = rdd.toDF("id", "name", "age")
    //
    //    val ds: Dataset[User] = df.as[User]
    //
    //    val df1: DataFrame = ds.toDF()
    //
    //    val rdd1: RDD[Row] = df1.rdd
    //
    //    rdd1.foreach(row => {
    //      println(row.getInt(0))
    //    })

    val value: RDD[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val value1: Dataset[User] = value.toDS()
    value1.rdd
    spark.stop()

  }

  @Test
  def test1(): Unit = {
    var array: Array[Any] = Array("user", 1, 2, 3)

    var sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()




  }

  //弱类型

  @Test
  def test2(): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val udaf = new MyAgeAvgFunction

    spark.udf.register("avgAge", udaf)

    val frame: DataFrame = spark.read.json("input/test.json")
    //
    frame.createOrReplaceTempView("user")

    spark.sql("select avgAge(age) from user").show
    //自定义聚合函数
    spark.stop()

  }

  @Test
  def test3(): Unit ={
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()


    import spark.implicits._
    //创建聚合函数对象
    val udaf = new MyAgeAvgClassFunction
    //将聚合函数转换为查询列
    val avgCol: TypedColumn[User, Double] = udaf.toColumn.name("avgAge")

    val frame: DataFrame = spark.read.json("input/test.json")
    val value: Dataset[User] = frame.as[User]
    value.select(avgCol).show()

    spark.stop()
  }


}
//声明用户自定义聚合函数
//1) 继承 .....
//2) 实现方法

class MyAgeAvgFunction extends UserDefinedAggregateFunction{

  //函数输入的数据结构
  override def inputSchema: StructType = {
    new StructType().add("age", LongType)
  }

  //计算时的数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum", LongType).add("count", LongType)
  }
  //函数返回的数据类型
  override def dataType: DataType = DoubleType

  //函数是否稳定
  override def deterministic: Boolean = true

  //计算之间的缓冲区的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  //将多个节点的缓冲区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //sum
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    //count
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)

  }

  //计算
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1).toDouble
  }
}

case class User(id: BigInt, name: String, age: BigInt)
case class AvgBuffer(var sum: BigInt, var count: Int)

class MyAgeAvgClassFunction extends Aggregator[User, AvgBuffer, Double]{

  //初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }

  override def reduce(b: AvgBuffer, a: User): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b

  }

  //缓冲区合并操作
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }
  //完成计算
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }
//自定义的类型就用 product
  //基本数据类型用scalaxxxx
  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}