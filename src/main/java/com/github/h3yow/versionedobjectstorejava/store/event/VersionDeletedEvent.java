package com.github.h3yow.versionedobjectstorejava.store.event;

/**
 * 版本删除事件：仅当版本过了保留期且无法务锁定时才可提交。
 */
public record VersionDeletedEvent(
        long seq,
        String space,
        String objectId,
        long version,
        String requester,
        long deletedAtEpochMillis
) implements Event {
}
