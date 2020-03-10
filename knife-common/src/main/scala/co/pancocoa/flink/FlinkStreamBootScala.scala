package co.pancocoa.flink

import co.pancocoa.flink.processor.IStreamProcessorScala
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Flink Stream Job Scala 启动入口，exactly once，checkpoint interval 5 min
 *
 * @author wangzhao
 * @date 2020/3/10
 */
object FlinkStreamBootScala {
  private val ARG_PROCESSOR_CLASS = "processor"
  private val ARG_JOB_NAME = "jobName"
  private val ARG_CHECKPOINT_INTERVAL_MILLIS = "checkpoint"

  /**
   * 默认的 checkpoint 周期为 5 min
   */
  private val DEFAULT_CHECKPOINT_INTERVAL_MILLIS = 5 * 60000

  @throws[IllegalAccessException]
  @throws[ClassNotFoundException]
  @throws[InstantiationException]
  def main(args: Array[String]): Unit = {
    // 1. 解析 args
    val params = ParameterTool.fromArgs(args)
    // 1.1 从 args 从获取 processor class
    val processorClazz = params.get(ARG_PROCESSOR_CLASS)
    // 1.2 从 args 从获取 job name
    val jobName = params.get(ARG_JOB_NAME, processorClazz)
    // 1.3 从 args 从获取 checkpoint interval(ms)
    val ckIntervalMillis = params.getLong(ARG_CHECKPOINT_INTERVAL_MILLIS, DEFAULT_CHECKPOINT_INTERVAL_MILLIS)
    try {
      if (StringUtils.isBlank(processorClazz)) throw new IllegalArgumentException("Flink Stream Job 启动失败：启动参数必须要包含 processor，args：" + params.toMap)
      // 1.4 根据 processor class 名称获取 processor
      val processor = Class.forName(processorClazz).newInstance.asInstanceOf[IStreamProcessorScala]

      // 2 创建 & 初始化 env
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.enableCheckpointing(ckIntervalMillis)

      // 3. 执行 processor.run 方法
      processor.run(env, params)

      // 4. 执行 Job
      env.execute(jobName)
    } catch {
      case e@(_: IllegalAccessException | _: InstantiationException | _: ClassNotFoundException) =>
        System.err.printf("Flink Stream Job 启动失败：获取 Processor 失败！args：%s\n", params.toMap)
        throw e
      case e: Exception =>
        System.err.printf("Flink Stream Job 启动失败！args：%s\n", params.toMap)
        throw e
    }
  }
}
