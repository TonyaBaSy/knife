package co.pancocoa.utils;

import co.pancocoa.flink.fs.CompressionType;
import co.pancocoa.flink.fs.RollTimeBucketer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.fs.SequenceFileWriter;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.concurrent.TimeUnit;

import static co.pancocoa.utils.Constants.Bucketing.DEFAULT_DATE_FORMAT;

/**
 * 创建 BucketingSink, 支持压缩类型的 SequenceFile 输出(gzip/snappy/lz4/bzip)和 String 类型.
 *
 * @author wangzhao
 * @date 2020/3/7
 */
public class BucketUtils {

    private static String dateFormat = DEFAULT_DATE_FORMAT;
    private static long intervalMin = 15L;

    /**
     * 设置「时间格式」
     *
     * @param format 默认是 yyyyMMdd/HH/mm
     */
    public static void setDateFormat(String format) {
        BucketUtils.dateFormat = format;
    }

    /**
     * 设置「滚动周期」
     *
     * @param intervalMin 默认是 15 min
     */
    public static void setIntervalMin(long intervalMin) {
        BucketUtils.intervalMin = intervalMin;
    }

    /**
     * gzip 压缩的 SequenceFile，key：Text，value：BytesWritable
     */
    public static BucketingSink<Tuple2<Text, BytesWritable>> gzip(String path) {
        return gzip(path, intervalMin);
    }

    /**
     * snappy 压缩的 SequenceFile，key：Text，value：BytesWritable
     */
    public static BucketingSink<Tuple2<Text, BytesWritable>> snappy(String path) {
        return snappy(path, intervalMin);
    }

    /**
     * lz4 压缩的 SequenceFile，key：Text，value：BytesWritable
     */
    public static BucketingSink<Tuple2<Text, BytesWritable>> lz4(String path) {
        return lz4(path, intervalMin);
    }

    /**
     * bzip2 压缩的 SequenceFile，key：Text，value：BytesWritable
     */
    public static BucketingSink<Tuple2<Text, BytesWritable>> bzip2(String path) {
        return bzip2(path, intervalMin);
    }

    public static BucketingSink<Tuple2<Text, BytesWritable>> gzip(String path, long intervalMin) {
        return compress(path, CompressionType.GZIP, intervalMin, Text.class, BytesWritable.class);
    }

    public static BucketingSink<Tuple2<Text, BytesWritable>> snappy(String path, long intervalMin) {
        return compress(path, CompressionType.SNAPPY, intervalMin, Text.class, BytesWritable.class);
    }

    public static BucketingSink<Tuple2<Text, BytesWritable>> lz4(String path, long intervalMin) {
        return compress(path, CompressionType.LZ4, intervalMin, Text.class, BytesWritable.class);
    }

    public static BucketingSink<Tuple2<Text, BytesWritable>> bzip2(String path, long intervalMin) {
        return compress(path, CompressionType.BZIP2, intervalMin, Text.class, BytesWritable.class);
    }

    public static <K extends Writable, V extends Writable> BucketingSink<Tuple2<K, V>> compress(String path,
                                                                                                CompressionType type,
                                                                                                long intervalMin,
                                                                                                Class<K> keyClazz,
                                                                                                Class<V> valClazz) {
        BucketingSink<Tuple2<K, V>> sink = new BucketingSink<>(path);
        sink.setBucketer(new RollTimeBucketer<>(dateFormat, intervalMin, TimeUnit.MINUTES))
                .setWriter(new SequenceFileWriter<>(type.getClazz(), SequenceFile.CompressionType.BLOCK))
                .setBatchSize(512 * 1024 * 1024) // 512 MB 滚动一次
                .setBatchRolloverInterval(20 * 60_000) // 20 min 滚动一次
                .setPartSuffix(type.getName());

        return sink;
    }

    /**
     * string 类型的 BucketingSink
     */
    public static BucketingSink<String> string(String path) {
        return string(path, 15);
    }

    public static BucketingSink<String> string(String path, long intervalMin) {
        BucketingSink<String> sink = new BucketingSink<>(path);
        sink.setBucketer(new RollTimeBucketer<>(dateFormat, intervalMin, TimeUnit.MINUTES))
                .setWriter(new StringWriter<>())
                .setBatchSize(512 * 1024 * 1024) // 512 MB 滚动一次
                .setBatchRolloverInterval(20 * 60_000); // 20 min 滚动一次

        return sink;
    }
}
