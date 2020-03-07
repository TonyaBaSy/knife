package tony.pancocoa

import org.apache.flink.streaming.api.scala._

/**
 * nc -lk 9000
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val hostname = "localhost"
    val port = 9000

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text:DataStream[String] = env.socketTextStream(hostname, port, '\n')
    val counts: DataStream[(String, Int)] = text
      // split up the lines in pairs (2-tuples) containing: (word,1)
      .flatMap(_.split(" "))
      .map((_, 1))
      // group by the tuple field "0" and sum up tuple field "1"
      .keyBy(0)
      .sum(1)

    counts.print()
  }
}
