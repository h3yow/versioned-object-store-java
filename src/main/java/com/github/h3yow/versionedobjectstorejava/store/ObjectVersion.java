package com.github.h3yow.versionedobjectstorejava.store;

/**
 * 一个不可变版本的可追溯元数据视图。
 */
public record ObjectVersion(
        String space,
        String objectId,
        long version,
        String sha256,
        long sizeBytes,
        String ownerPrincipal,
        long retentionUntilEpochMillis,
        boolean legalHold,
        long committedAtEpochMillis,
        boolean deleted
) {
}
