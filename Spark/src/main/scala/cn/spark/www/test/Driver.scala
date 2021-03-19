package cn.spark.www.test

import java.net.Socket

/**
 * @author Hale Lv
 * @project Hadoop
 * @created 2021-03-12 15:47
 */
object Driver {
  def main(args: Array[String]): Unit = {
    val client = new Socket("192.168.123.1", 9900)
    val out = client.getOutputStream

    out.write(2)
    out.flush()
    out.close()

    client.close()
  }
}
