package co.pancocoa.flink.fs;

import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.hadoop.fs.Path;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import static co.pancocoa.utils.Constants.Bucketing.DEFAULT_DATE_FORMAT;

/**
 * 滚动 Bucketer, 默认滚动周期为 5 min.
 *
 * @author wangzhao
 * @date 2020/3/7
 */
public class RollTimeBucketer<T> implements Bucketer<T> {
    private static final long serialVersionUID = 1L;

    /**
     * 默认的滚动周期为 5 min;
     */
    private static final long DEFAULT_ROLL_INTERVAL_MILLIS = 5 * 60_000;

    private final String formatString;
    private final long rollIntervalMillis;

    private final DateTimeFormatter formatter;

    public RollTimeBucketer() {
        this(DEFAULT_DATE_FORMAT, ZoneId.systemDefault(), DEFAULT_ROLL_INTERVAL_MILLIS);
    }

    public RollTimeBucketer(String formatString, long rollInterval, TimeUnit unit) {
        this(formatString, ZoneId.systemDefault(), unit.toMillis(rollInterval));
    }

    public RollTimeBucketer(String formatString, ZoneId zoneId, long rollIntervalMillis) {
        this.formatString = formatString;
        this.rollIntervalMillis = rollIntervalMillis;

        this.formatter = DateTimeFormatter.ofPattern(this.formatString).withZone(zoneId);
    }

    @Override
    public Path getBucketPath(Clock clock, Path baseDir, T t) {
        long currMillis = clock.currentTimeMillis() / this.rollIntervalMillis * this.rollIntervalMillis;
        String bucketId = this.formatter.format(Instant.ofEpochMilli(currMillis));

        return new Path(baseDir, bucketId);
    }
}
