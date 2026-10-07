package com.github.h3yow.versionedobjectstorejava.store.model;

import java.util.List;

/**
 * 一次批量提交：多个对象的新增/覆盖写入作为一个整体提交。
 *
 * @param batchId 客户端提供的批量提交标识；用于崩溃恢复与幂等重试，
 *                同一标识重试返回稳定一致的结果，不重复产生版本或占用空间
 * @param items   写入项列表（非空；同一 key 不得出现两次）
 */
public record BatchCommit(String batchId, List<BatchItem> items) {
}
