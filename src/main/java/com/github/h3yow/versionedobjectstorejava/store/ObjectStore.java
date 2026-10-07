package com.github.h3yow.versionedobjectstorejava.store;

import com.github.h3yow.versionedobjectstorejava.store.model.BatchCommit;
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
     * 批量提交：多个对象的新增/覆盖写入作为一个整体，要么全部可见要么全部不可见。
     *
     * <p>每个对象仍按单对象规则产生不可变版本，版本号照旧单调递增。整批只按净新增
     * 内容计配额，超限则整批拒绝且不留残余占用；批内与跨批相同内容只存一份。
     * 任一对象越权、版本条件冲突或违反保留规则时整批拒绝，原因以
     * {@link StoreException#code()} 区分。以同一 batchId 重试时幂等：
     * 不重复产生版本、不重复占用空间，返回稳定一致的结果。</p>
     */
    BatchResult commitBatch(String bucket, BatchCommit batch, String principal);

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
