package com.github.h3yow.versionedobjectstorejava.store.model;

import java.time.Instant;

/**
 * 保留策略。
 *
 * @param retentionUntil 保留截止时刻；{@code null} 表示无保留期
 * @param complianceLock 合规锁定（比保留期更强，锁定期间任何删除/回收都被拒绝）
 */
public record RetentionPolicy(Instant retentionUntil, boolean complianceLock) {

    public static final RetentionPolicy NONE = new RetentionPolicy(null, false);

    public static RetentionPolicy until(Instant instant) {
        return new RetentionPolicy(instant, false);
    }
}
