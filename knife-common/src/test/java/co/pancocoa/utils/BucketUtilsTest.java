package co.pancocoa.utils;

import co.pancocoa.flink.util.BucketUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * @author wangzhao
 * @date 2020/3/7
 */
class BucketUtilsTest {
    private static final Logger logger = LoggerFactory.getLogger(BucketUtilsTest.class);

    @Test
    public void string() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000);

        BucketingSink<Tuple2<Text, BytesWritable>> sink = BucketUtils.gzip("hdfs://hadoop01:9000/user/test/output");
//        sink.setUseTruncate(false);

        env.socketTextStream("localhost", 8888)
                .map(new RichMapFunction<String, Tuple2<Text, BytesWritable>>() {
                    @Override
                    public Tuple2<Text, BytesWritable> map(String s) throws Exception {
                        String[] ss = s.split(",");
                        logger.info("测试日志：{}", Arrays.toString(ss));
                        return Tuple2.of(new Text(ss[0]), new BytesWritable(ss[1].getBytes(StandardCharsets.UTF_8)));
                    }
                }).addSink(sink);

        env.execute("bucket-sink-test");
    }
}