package com.github.h3yow.versionedobjectstorejava.store.model;

/**
 * 批量提交中的单个对象写入项。
 *
 * @param key     对象键（同一批内不得重复）
 * @param request 写入请求，语义与单对象 {@code put} 完全一致
 */
public record BatchItem(String key, WriteRequest request) {

    public static BatchItem of(String key, byte[] content) {
        return new BatchItem(key, WriteRequest.of(content));
    }
}
