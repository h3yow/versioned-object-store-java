package com.github.h3yow.versionedobjectstorejava.config;

import com.github.h3yow.versionedobjectstorejava.store.BlobStore;
import com.github.h3yow.versionedobjectstorejava.store.Journal;
import com.github.h3yow.versionedobjectstorejava.store.ObjectStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

/**
 * 装配本地对象存储的磁盘组件。
 */
@Configuration
@EnableConfigurationProperties(StoreProperties.class)
public class StoreAutoConfiguration {

    @Bean
    public BlobStore blobStore(StoreProperties properties) {
        return new BlobStore(Path.of(properties.getRoot()));
    }

    @Bean
    public Journal journal(StoreProperties properties) {
        return new Journal(Path.of(properties.getRoot()));
    }

    @Bean
    public ObjectStore objectStore(BlobStore blobStore, Journal journal, StoreProperties properties) {
        // 构造器内部已完成日志重放与磁盘协调恢复
        return new ObjectStore(blobStore, journal, properties.getMaxObjectBytes());
    }
}
