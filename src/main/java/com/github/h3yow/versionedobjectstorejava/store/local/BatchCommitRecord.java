package com.github.h3yow.versionedobjectstorejava.store.local;

import com.github.h3yow.versionedobjectstorejava.store.model.VersionInfo;

import java.time.Instant;
import java.util.List;

/**
 * 批量提交的持久化提交记录，存放于 {@code root/batches/<batchId>.json}。
 *
 * <p>该文件的原子落盘即整批的<b>提交点</b>：落盘前崩溃整批不算数；
 * 落盘后崩溃由重启恢复把各对象 HEAD 补齐到记录中的版本。
 * 同时它也是幂等重试的结果存根，同一 batchId 重试直接回放。</p>
 *
 * @param batchId     批量提交标识
 * @param fingerprint 提交内容的规范指纹（防止同一标识被不同内容复用）
 * @param bucket      目标存储空间
 * @param principal   提交者
 * @param committedAt 提交时间
 * @param versions    各对象本次提交产生的版本元数据
 */
record BatchCommitRecord(String batchId, String fingerprint, String bucket,
                         String principal, Instant committedAt, List<VersionInfo> versions) {
}
