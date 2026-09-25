package com.github.h3yow.versionedobjectstorejava.store.event;

/**
 * 版本提交事件：不可变版本的完整元数据。
 *
 * @param version                 单调递增版本号（同一对象内）
 * @param expectedVersion         客户端携带的乐观锁条件，-1 表示无条件（默认语义）
 * @param contentDeduplicated     本次提交是否复用了已有内容块（true 表示未新增磁盘占用）
 * @param ownerPrincipal          版本/对象属主
 * @param retentionUntilEpochMillis 保留期截止时间戳（毫秒），0 表示无保留期
 * @param legalHold               合规锁定标记，锁定期间禁止删除
 * @param committedAtEpochMillis  提交时间戳（毫秒）
 */
public record VersionCommittedEvent(
        long seq,
        String space,
        String objectId,
        long version,
        String sha256,
        long sizeBytes,
        long expectedVersion,
        boolean contentDeduplicated,
        String ownerPrincipal,
        long retentionUntilEpochMillis,
        boolean legalHold,
        long committedAtEpochMillis
) implements Event {
}
