package co.pancocoa.flink;

import co.pancocoa.flink.processor.IBatchProcessor;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Flink Batch Job 启动入口
 *
 * @author wangzhao
 * @date 2020/3/10
 */
public class FlinkBatchBootJava {
    private static final String ARG_PROCESSOR_CLASS = "processor";

    public static void main(String[] args) throws IllegalAccessException, ClassNotFoundException, InstantiationException {
        // 1. 解析 args
        ParameterTool params = ParameterTool.fromArgs(args);
        // 1.1 从 args 从获取 processor class
        String processorClazz = params.get(ARG_PROCESSOR_CLASS);

        try {
            if (StringUtils.isBlank(processorClazz)) {
                throw new IllegalArgumentException("Flink Batch Job 启动失败：启动参数必须要包含 processor，args：" + params.toMap());
            }
            // 1.4 根据 processor class 名称获取 processor
            IBatchProcessor processor = (IBatchProcessor) Class.forName(processorClazz).newInstance();

            // 2 创建 & 初始化 env
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            // 3. 执行 processor.run 方法
            processor.run(env, params);
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
            System.err.printf("Flink Batch Job 启动失败：获取 Processor 失败！args：%s\n", params.toMap());
            throw e;
        } catch (Exception e) {
            System.err.printf("Flink Batch Job 启动失败！args：%s\n", params.toMap());
            throw e;
        }
    }
}
