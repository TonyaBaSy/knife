package co.pancocoa.flink

import co.pancocoa.flink.processor.IBatchProcessorScala
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * Flink Batch Job Scala 启动入口
 *
 * @author wangzhao
 * @date 2020/3/10
 */
object FlinkBatchBootScala {

  private val ARG_PROCESSOR_CLASS = "processor"

  def main(args: Array[String]): Unit = {
    // 1. 解析 args
    val params: ParameterTool = ParameterTool.fromArgs(args)
    // 1.1 从 args 从获取 processor class
    val processorClazz: String = params.get(ARG_PROCESSOR_CLASS)
    try {
      if (StringUtils.isBlank(processorClazz)) throw new IllegalArgumentException("Flink Batch Job 启动失败：启动参数必须要包含 processor，args：" + params.toMap)
      // 1.4 根据 processor class 名称获取 processor
      val processor = Class.forName(processorClazz).newInstance.asInstanceOf[IBatchProcessorScala]

      // 2 创建 & 初始化 env
      val env = ExecutionEnvironment.getExecutionEnvironment

      // 3. 执行 processor.run 方法
      processor.run(env, params)
    } catch {
      case e@(_: IllegalAccessException | _: InstantiationException | _: ClassNotFoundException) =>
        System.err.printf("Flink Batch Job 启动失败：获取 Processor 失败！args：%s\n", params.toMap)
        throw e
      case e: Exception =>
        System.err.printf("Flink Batch Job 启动失败！args：%s\n", params.toMap)
        throw e
    }
  }
}
