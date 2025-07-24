package com.wikimedia.kafka.messaging;

public abstract class Topics {

    public static final String RECENT_CHANGE = "wikimedia.recentchange";
    public static final String SNAPPY_RECENT_CHANGE = "wikimedia.recentchange.snappy";
    public static final String LZ4_RECENT_CHANGE = "wikimedia.recentchange.lz4";
    public static final String ZSTD_RECENT_CHANGE = "wikimedia.recentchange.zstd";
    public static final String GZIP_RECENT_CHANGE = "wikimedia.recentchange.gzip";
}
