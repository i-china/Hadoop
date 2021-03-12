package cn.spark.www

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hale Lv
 * @project Hadoop
 * @created 2021-03-12 09:17
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    // Application
    // Spark 框架

    // TODO  建立和Spark框架的连接
//    JDBC： Connection
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")

    val sc = new SparkContext(sparkConf)

    //    TODO 执行业务操作
    /**
     *   @Description: main
     *   @param: [args] 
     *   @return: void
     */
    val lines = sc.textFile("C:\\Users\\iFaithFreedom\\IdeaProjects\\BigData\\GitHub\\hadoop\\Spark\\src\\main\\datas")

    val words = lines.flatMap(_.split(" "))

    val wordGroup = words.groupBy(word => word)

    val wordToGroup = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    val array = wordToGroup.collect()
    array.foreach(println)



    sc.stop()
  }
}
