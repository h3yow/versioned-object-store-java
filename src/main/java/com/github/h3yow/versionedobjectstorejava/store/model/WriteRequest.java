package com.github.h3yow.versionedobjectstorejava.store.model;

/**
 * 一次版本化写入请求。
 *
 * @param content         对象内容（非空）
 * @param expectedVersion 期望的当前版本号；为 {@code null} 时采用确定且稳定的默认语义：
 *                        无条件追加新版本（不检查当前版本）
 * @param checksumSha256  客户端声明的 SHA-256（十六进制小写）；为 {@code null} 时不校验
 */
public record WriteRequest(byte[] content, Long expectedVersion, String checksumSha256) {

    public static WriteRequest of(byte[] content) {
        return new WriteRequest(content, null, null);
    }
}
