package com.github.h3yow.versionedobjectstorejava.store;

/**
 * 一次版本化写入请求。
 *
 * @param expectedVersion 乐观锁条件：当前最新版本必须等于该值；-1 表示无条件（默认语义），
 *                        0 表示对象必须尚不存在
 * @param expectedSha256  客户端声明的内容 SHA-256（十六进制小写），必填，用于校验和判定
 * @param retentionMillis 保留期时长（从提交时刻起），0 表示不设保留期
 * @param legalHold       是否设置合规锁定
 */
public record PutRequest(
        String space,
        String objectId,
        String principal,
        byte[] content,
        long expectedVersion,
        String expectedSha256,
        long retentionMillis,
        boolean legalHold
) {
}
