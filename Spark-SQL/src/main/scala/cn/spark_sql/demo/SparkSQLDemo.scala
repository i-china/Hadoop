package cn.spark_sql.demo

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.spark_project.jetty.server.Authentication.User

/**
 * @author Hale Lv
 * @project Hadoop
 * @created 2021-03-12 17:26
 */
object SparkSQLDemo {
  def main(args: Array[String]): Unit = {
    //    创建上下文环境配置对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_HALE")
    //    创建 SparkSession对象
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //    TODO  RDD=>DataFrame => DataSet 转换需要引入隐式转换规格，否则无法转换
    import spark.implicits._
    //    读取json文件，创建DataFrame {"username":"hale","age":23}
    val df = spark.read.json("/datas/user.json")
    //    df.show()
    // SQL 风格语法
    df.createOrReplaceTempView("user")
    // DSL 风格语法
//    df.select("username","age").show()

//    RDD
    val rdd1 = spark.sparkContext.makeRDD(List((1, "hale", 30), (2, "wow", 23), (3, "root")))

    // DataFrame
    val df1 = rdd1.toDF("id", "name", "age")
    //    df1.show()

    //    DataSet

    val ds1: Dataset[User] = df1.as[User]

    //    ds1.show()

    //    DataFrame
    val df2 = ds1.toDF()

    val rdd2 = df2.rdd

    /**
     *   @Description: main
     *   @param: [args] 
     *   @return: void
     */
    rdd1.map{
      case (id,name,age)=>User(id,name,age)
    }.toDS()


    ds1.rdd

    spark.stop()
  }
}

case class User(id:Int, name:String, age:Int)