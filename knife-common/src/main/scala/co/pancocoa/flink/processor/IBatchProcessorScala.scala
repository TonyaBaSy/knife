package co.pancocoa.flink.processor

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * Flink Batch Processor Scala 接口
 *
 * @author wangzhao
 * @date 2020/3/10
 */
trait IBatchProcessorScala {

  @throws[Exception]
  def run(env: ExecutionEnvironment, params: ParameterTool): Unit
}
