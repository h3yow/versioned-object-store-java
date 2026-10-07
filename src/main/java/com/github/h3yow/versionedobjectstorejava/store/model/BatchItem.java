package com.github.h3yow.versionedobjectstorejava.store.model;

/**
 * 批量提交中的单个对象写入项，语义与单对象 {@link WriteRequest} 完全一致。
 *
 * @param key             对象键
 * @param request         写入请求（内容、版本条件、校验和）
 */
public record BatchItem(String key, WriteRequest request) {

    public static BatchItem of(String key, byte[] content) {
        return new BatchItem(key, WriteRequest.of(content));
    }
}
