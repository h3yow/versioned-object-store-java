package com.github.h3yow.versionedobjectstorejava.store;

/**
 * 一次原子读取的结果：版本元数据与其内容保证属于同一个不可变版本。
 */
public record VersionContent(ObjectVersion version, byte[] content) {
}
