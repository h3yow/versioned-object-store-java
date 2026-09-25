package com.github.h3yow.versionedobjectstorejava.store;

/**
 * 所有写入/读取/删除被拒绝时的可区分原因。
 */
public enum RejectReason {
    EMPTY_CONTENT,
    CHECKSUM_MISMATCH,
    OBJECT_TOO_LARGE,
    QUOTA_EXCEEDED,
    SPACE_NOT_FOUND,
    OBJECT_NOT_FOUND,
    VERSION_NOT_FOUND,
    VERSION_CONFLICT,
    ACCESS_DENIED,
    RETENTION_ACTIVE,
    LEGAL_HOLD_ACTIVE,
    BLOB_IN_USE
}
