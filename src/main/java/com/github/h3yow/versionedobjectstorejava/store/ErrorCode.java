package com.github.h3yow.versionedobjectstorejava.store;

/**
 * 可区分的拒绝原因。
 */
public enum ErrorCode {
    /** 客户端声明的校验和与实际内容不一致 */
    CHECKSUM_MISMATCH,
    /** 空内容写入被拒绝 */
    EMPTY_CONTENT,
    /** 超出存储容量配额 */
    QUOTA_EXCEEDED,
    /** 越权访问 */
    ACCESS_DENIED,
    /** 对象或版本不存在 */
    OBJECT_NOT_FOUND,
    /** 版本条件冲突（乐观并发控制失败） */
    VERSION_CONFLICT,
    /** 保留期内禁止删除/回收 */
    RETENTION_ACTIVE,
    /** 合规锁定内禁止删除/回收 */
    COMPLIANCE_LOCKED
}
