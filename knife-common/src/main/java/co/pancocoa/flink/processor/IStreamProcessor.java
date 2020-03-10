package co.pancocoa.flink.processor;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink Stream Processor 接口
 *
 * @author wangzhao
 * @date 2020/3/10
 */
public interface IStreamProcessor {

    void run(StreamExecutionEnvironment env, ParameterTool params);
}
