package com.github.h3yow.versionedobjectstorejava.store.model;

import java.util.List;

/**
 * 批量提交的稳定结果。同一 batchId 的重试返回完全一致的内容。
 *
 * @param batchId          批量提交标识
 * @param versions         各对象本次提交产生的不可变版本元数据（与提交项一一对应）
 * @param idempotentReplay {@code true} 表示本次调用是对已提交批次的幂等重放，
 *                         未产生新版本、未占用新空间
 */
public record BatchResult(String batchId, List<VersionInfo> versions, boolean idempotentReplay) {
}
