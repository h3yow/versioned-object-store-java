package com.github.h3yow.versionedobjectstorejava.store;

import com.github.h3yow.versionedobjectstorejava.store.model.BatchItem;
import com.github.h3yow.versionedobjectstorejava.store.model.BatchResult;
import com.github.h3yow.versionedobjectstorejava.store.model.RetentionPolicy;
import com.github.h3yow.versionedobjectstorejava.store.model.VersionInfo;
import com.github.h3yow.versionedobjectstorejava.store.model.WriteRequest;

import java.util.List;

/**
 * 本地磁盘版本化对象存储核心。
 *
 * <p>所有操作以 principal 身份执行并做访问控制；所有拒绝均以携带
 * {@link ErrorCode} 的 {@link StoreException} 抛出，原因可区分。</p>
 */
public interface ObjectStore {

    /** 创建存储空间（需要 ADMIN 由调用方先行授予创建者）。 */
    void createBucket(String bucket, String ownerPrincipal);

    /**
     * 版本化写入。同一内容去重存储；成功提交返回不可变版本元数据。
     * 未携带 expectedVersion 时默认语义为无条件追加新版本。
     */
    VersionInfo put(String bucket, String key, WriteRequest request, String principal);

    /**
     * 原子批量提交：多个对象的新增/覆盖对外表现为一个整体，要么全部可见，
     * 要么全部不可见。每个对象按原有规则产生不可变、单调递增的新版本。
     *
     * <p>容量按整批净新增内容一次性核算；批内及与已有内容之间均按 SHA-256 去重；
     * 任一项的权限、版本条件或保留规则不满足则整批拒绝，不残留已占空间。
     * 同一 {@code batchId} 的重试是幂等的：不会重复生成版本或占用空间，
     * 返回首次提交的稳定结果。</p>
     *
     * @param batchId   客户端提供的批量提交标识（同一 bucket 内唯一，重试须复用）
     * @param bucket    目标存储空间
     * @param items     批量写入项（不可为空，批内 key 不可重复）
     * @param principal 提交者身份（对每个对象都需具备 WRITER 权限）
     */
    BatchResult putBatch(String batchId, String bucket, List<BatchItem> items, String principal);

    /** 读取最新已提交版本的内容。 */
    byte[] get(String bucket, String key, String principal);

    /** 读取指定版本的内容。 */
    byte[] get(String bucket, String key, long version, String principal);

    /** 列出对象的全部已提交版本（按版本号升序）。 */
    List<VersionInfo> listVersions(String bucket, String key, String principal);

    /** 当前已提交的最新版本号；对象不存在时抛 OBJECT_NOT_FOUND。 */
    long headVersion(String bucket, String key, String principal);

    /** 为对象后续写入设置保留策略（需要 WRITER）。 */
    void setRetention(String bucket, String key, RetentionPolicy policy, String principal);

    /**
     * 删除指定版本（需要 WRITER）。保留期内或合规锁定时拒绝；
     * 仅当内容不再被任何版本引用时才回收底层内容块。
     */
    void deleteVersion(String bucket, String key, long version, String principal);

    /** 回收不再被任何版本引用的内容块，返回回收的字节数。 */
    long garbageCollect(String principal);
}
