package cn.spark.www.test

import java.net.ServerSocket

/**
 * @author Hale Lv
 * @project Hadoop
 * @created 2021-03-12 15:59
 */
object Executor {
  def main(args: Array[String]): Unit = {
    val server = new ServerSocket(9900)
    println("服务器启动，等待接收数据")

    val client = server.accept()
    val in = client.getInputStream

    val i = in.read()
    println("接收到客户端发送的数据")

    in.close()
    client.close()
    server.close()
  }
}
