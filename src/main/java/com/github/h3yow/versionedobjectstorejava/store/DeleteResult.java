package com.github.h3yow.versionedobjectstorejava.store;

/**
 * 删除成功结果。
 *
 * @param blobReclaimed 内容块是否已被物理回收（无任何存活版本引用时才可能为 true）
 */
public record DeleteResult(
        String space,
        String objectId,
        long version,
        boolean blobReclaimed
) {
}
