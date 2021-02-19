package cn.spark.com

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}



/**
 * @Author: HaleLv
 * @Date: 2021-02-18
 * @ProjectName Hadoop
 */

object WordCount {
  def main(args: Array[String]) {
//    System.setProperty("hadoop.home.dir", "D:\\jdk\\hadoop-2.7.6")
    val resourcesPath = "src/main/resources/"

    val sparkConf = new SparkConf().setMaster("local").setAppName("sparkDemo");
    val sc = new SparkContext(sparkConf);

    val textFile = sc.textFile(resourcesPath + "word.txt")
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)


    val file = new File(resourcesPath + "word_result")
    if (file.exists()) {
      val listfiles = file.listFiles()
      listfiles.foreach(_.delete())
      file.delete()
    }
    counts.saveAsTextFile(resourcesPath +"word_result")

  }
}
