package co.pancocoa.flink.processor;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Flink Batch Processor 接口
 *
 * @author wangzhao
 * @date 2020/3/10
 */
public interface IBatchProcessor {
    void run(ExecutionEnvironment env, ParameterTool params);
}
