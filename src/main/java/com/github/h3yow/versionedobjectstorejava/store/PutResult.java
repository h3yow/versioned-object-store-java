package com.github.h3yow.versionedobjectstorejava.store;

/**
 * 提交成功结果。
 */
public record PutResult(
        String space,
        String objectId,
        long version,
        String sha256,
        boolean deduplicated
) {
}
