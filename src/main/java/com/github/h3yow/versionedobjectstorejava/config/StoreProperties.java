package com.github.h3yow.versionedobjectstorejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 本地对象存储的可配置项，前缀 {@code objectstore}。
 */
@ConfigurationProperties(prefix = "objectstore")
public class StoreProperties {

    /** 存储根目录（内容块与日志都落在此目录下）。 */
    private String root = "./data/objectstore";

    /** 单个对象内容的最大字节数。 */
    private long maxObjectBytes = 100L * 1024 * 1024;

    public String getRoot() {
        return root;
    }

    public void setRoot(String root) {
        this.root = root;
    }

    public long getMaxObjectBytes() {
        return maxObjectBytes;
    }

    public void setMaxObjectBytes(long maxObjectBytes) {
        this.maxObjectBytes = maxObjectBytes;
    }
}
