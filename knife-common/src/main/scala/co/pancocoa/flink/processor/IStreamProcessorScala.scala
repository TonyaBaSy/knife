package co.pancocoa.flink.processor

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Flink Stream Processor Scala 接口
 *
 * @author wangzhao
 * @date 2020/3/10
 */
trait IStreamProcessorScala {

  @throws[Exception]
  def run(env: StreamExecutionEnvironment, params: ParameterTool): Unit
}
