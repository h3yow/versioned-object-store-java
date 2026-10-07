package com.github.h3yow.versionedobjectstorejava.store.model;

import java.util.List;

/**
 * 一次批量提交的结果。整批要么全部成功（返回本结果），要么整体拒绝
 * （抛出携带 {@link com.github.h3yow.versionedobjectstorejava.store.ErrorCode} 的异常）。
 *
 * @param batchId  批量提交标识（客户端提供，重试时必须相同）
 * @param versions 本批为每个对象签发的新版本，顺序与请求项一一对应
 */
public record BatchResult(String batchId, List<VersionInfo> versions) {
}
