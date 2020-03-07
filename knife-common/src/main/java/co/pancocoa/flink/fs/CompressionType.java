package co.pancocoa.flink.fs;

public enum  CompressionType {
    GZIP(".gzip", "org.apache.hadoop.io.compress.GzipCodec"),
    SNAPPY(".snappy", "org.apache.hadoop.io.compress.SnappyCodec"),
    LZ4(".lz4", "org.apache.hadoop.io.compress.Lz4Codec"),
    BZIP2(".bzip2", "org.apache.hadoop.io.compress.BZip2Codec");

    private String name;
    private String clazz;

    CompressionType(String name, String clazz) {
        this.name = name;
        this.clazz = clazz;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getClazz() {
        return clazz;
    }

    public void setClazz(String clazz) {
        this.clazz = clazz;
    }
}
