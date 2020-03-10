package co.pancocoa.flink;

import co.pancocoa.flink.processor.IStreamProcessor;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink Stream Job 启动入口，exactly once，checkpoint interval 5 min
 *
 * @author wangzhao
 * @date 2020/3/10
 */
public class FlinkStreamBootJava {

    private static final String ARG_PROCESSOR_CLASS = "processor";
    private static final String ARG_JOB_NAME = "jobName";
    private static final String ARG_CHECKPOINT_INTERVAL_MILLIS = "checkpoint";

    /**
     * 默认的 checkpoint 周期为 5 min
     */
    private static final long DEFAULT_CHECKPOINT_INTERVAL_MILLIS = 5 * 60_000;

    public static void main(String[] args) throws IllegalAccessException, ClassNotFoundException, InstantiationException {
        // 1. 解析 args
        ParameterTool params = ParameterTool.fromArgs(args);
        // 1.1 从 args 从获取 processor class
        String processorClazz = params.get(ARG_PROCESSOR_CLASS);
        // 1.2 从 args 从获取 job name
        String jobName = params.get(ARG_JOB_NAME, processorClazz);
        // 1.3 从 args 从获取 checkpoint interval(ms)
        long ckIntervalMillis = params.getLong(ARG_CHECKPOINT_INTERVAL_MILLIS, DEFAULT_CHECKPOINT_INTERVAL_MILLIS);

        try {
            if (StringUtils.isBlank(processorClazz)) {
                throw new IllegalArgumentException("Flink Stream Job 启动失败：启动参数必须要包含 processor，args：" + params.toMap());
            }
            // 1.4 根据 processor class 名称获取 processor
            IStreamProcessor processor = (IStreamProcessor) Class.forName(processorClazz).newInstance();

            // 2 创建 & 初始化 env
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.enableCheckpointing(ckIntervalMillis);

            // 3. 执行 processor.run 方法
            processor.run(env, params);

            // 4. 执行 Job
            env.execute(jobName);
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
            System.err.printf("Flink Stream Job 启动失败：获取 Processor 失败！args：%s\n", params.toMap());
            throw e;
        } catch (Exception e) {
            System.err.printf("Flink Stream Job 启动失败！args：%s\n", params.toMap());
            e.printStackTrace();
        }
    }
}
