package com.github.h3yow.versionedobjectstorejava.store.model;

import java.time.Instant;

/**
 * 一个不可变已提交版本的元数据。
 *
 * @param bucket        存储空间
 * @param key           对象键
 * @param version       单调递增的版本号（从 1 开始）
 * @param sha256        内容 SHA-256（十六进制小写），同时是去重键
 * @param size          内容字节数
 * @param committedAt   提交时间
 * @param committedBy   提交者
 * @param retentionUntil 保留截止时刻；{@code null} 表示无保留期
 * @param complianceLock 合规锁定；锁定期间禁止删除与回收
 */
public record VersionInfo(String bucket, String key, long version, String sha256, long size,
                          Instant committedAt, String committedBy,
                          Instant retentionUntil, boolean complianceLock) {
}
