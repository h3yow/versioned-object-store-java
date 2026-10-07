
package com.github.h3yow.versionedobjectstorejava.store.local;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.h3yow.versionedobjectstorejava.store.ErrorCode;
import com.github.h3yow.versionedobjectstorejava.store.ObjectStore;
import com.github.h3yow.versionedobjectstorejava.store.StoreException;
import com.github.h3yow.versionedobjectstorejava.store.acl.LocalAccessControl;
import com.github.h3yow.versionedobjectstorejava.store.acl.Role;
import com.github.h3yow.versionedobjectstorejava.store.model.BatchItem;
import com.github.h3yow.versionedobjectstorejava.store.model.BatchResult;
import com.github.h3yow.versionedobjectstorejava.store.model.RetentionPolicy;
import com.github.h3yow.versionedobjectstorejava.store.model.VersionInfo;
import com.github.h3yow.versionedobjectstorejava.store.model.WriteRequest;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Local-disk only implementation of {@link ObjectStore}.
 *
 * Disk layout:
 *   root/blobs/<sha256>                       content-addressed immutable blobs (dedup)
 *   root/buckets/<bucket>/objects/<key>/versions/v-<n>.json   immutable version metadata
 *   root/buckets/<bucket>/objects/<key>/HEAD  last issued version number (commit point)
 *   root/buckets/<bucket>/objects/<key>/retention.json        retention policy
 *   root/batches/<batchId>/journal.json       prepared batch journal (plans + request identity)
 *   root/batches/<batchId>/committed          batch commit marker (atomic visibility point)
 *
 * Commit protocol (single object, under the global commit lock): validate -> write new blob
 * (tmp + fsync + atomic rename, quota accounted) -> write version metadata
 * tmp + fsync -> atomic rename to v-<n>.json -> atomic HEAD update -> fsync dir.
 * HEAD is the commit point: readers only accept version files <= HEAD, so no
 * half-committed state is ever visible.
 *
 * Batch protocol (putBatch, also under the global commit lock so two commits never
 * interleave): validate the whole batch (permissions, content, version conditions,
 * retention, aggregate quota on net-new bytes) -> write all net-new blobs (dedup
 * within batch and against existing content) -> durably write the PREPARED journal
 * -> write every item's v-<n>.json -> advance every involved object's HEAD
 * (batch versions stay invisible: readers additionally require the batch marker)
 * -> atomically create the 'committed' marker, which is the single atomic
 * visibility point: all objects of the batch become visible at once.
 *
 * Crash recovery (at construction): delete all *.tmp files; roll forward every
 * prepared/committed batch journal to completion (rewrite missing metadata,
 * advance HEADs, create the marker) so a crash mid-batch ends in all-committed;
 * then delete dangling version files with version > HEAD (belong to attempts that
 * never reached a durable journal). Blobs not referenced by any committed version
 * are reclaimed by garbageCollect.
 */
public class LocalObjectStore implements ObjectStore {

    private static final ObjectMapper JSON = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final Path blobsDir;
    private final Path bucketsDir;
    private final Path batchesDir;
    private final long quotaBytes;
    private final LocalAccessControl accessControl;

    /** Global lock serializing every mutating commit (single puts and batches). */
    private final ReentrantLock commitLock = new ReentrantLock();
    /** Global lock guarding blob creation and quota accounting. */
    private final Object blobLock = new Object();
    private final AtomicLong usedBytes;
    /** Batch ids whose commit marker exists (atomic visibility registry). */
    private final Set<String> committedBatches = ConcurrentHashMap.newKeySet();

    /** Test hook: fired after version metadata is durable but before HEAD update. */
    private volatile Runnable commitHook = null;
    /** Test hook: fired inside a batch after blobs are durable but before the journal is written. */
    private volatile Runnable batchPreJournalHook = null;
    /** Test hook: fired inside a batch after HEADs advance but before the commit marker. */
    private volatile Runnable batchPreCommitHook = null;
    /** Test hook: fired inside a batch right after the commit marker is durable. */
    private volatile Runnable batchPostCommitHook = null;

    public LocalObjectStore(Path root, long quotaBytes, LocalAccessControl accessControl) {
        this.blobsDir = root.resolve("blobs");
        this.bucketsDir = root.resolve("buckets");
        this.batchesDir = root.resolve("batches");
        this.quotaBytes = quotaBytes;
        this.accessControl = accessControl;
        try {
            Files.createDirectories(blobsDir);
            Files.createDirectories(bucketsDir);
            Files.createDirectories(batchesDir);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to initialize store root " + root, e);
        }
        recoverUncommittedState();
        this.usedBytes = new AtomicLong(sumBlobBytes());
    }

    /** Test only: inject a mid-commit crash hook. */
    public void setCommitHookForTesting(Runnable hook) {
        this.commitHook = hook;
    }

    /** Test only: inject a crash hook before the batch journal is made durable. */
    public void setBatchPreJournalHookForTesting(Runnable hook) {
        this.batchPreJournalHook = hook;
    }

    /** Test only: inject a crash hook before the batch commit marker. */
    public void setBatchPreCommitHookForTesting(Runnable hook) {
        this.batchPreCommitHook = hook;
    }

    /** Test only: inject a crash hook after the batch commit marker. */
    public void setBatchPostCommitHookForTesting(Runnable hook) {
        this.batchPostCommitHook = hook;
    }

    // ---------------------------------------------------------------- bucket

    @Override
    public void createBucket(String bucket, String ownerPrincipal) {
        try {
            Files.createDirectories(bucketDir(bucket));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to create bucket " + bucket, e);
        }
        accessControl.grant(bucket, ownerPrincipal, Role.ADMIN);
    }

    // ------------------------------------------------------------------ put

    @Override
    public BatchResult putBatch(String batchId, String bucket, List<BatchItem> items, String principal) {
        if (batchId == null || batchId.isBlank()) {
            throw new StoreException(ErrorCode.BATCH_INVALID, "batchId must not be blank");
        }
        if (items == null || items.isEmpty()) {
            throw new StoreException(ErrorCode.BATCH_INVALID, "batch must contain at least one item");
        }
        accessControl.require(bucket, principal, Role.WRITER);
        requireBucket(bucket);

        commitLock.lock();
        try {
            Path journalFile = batchDir(batchId).resolve("journal.json");
            BatchJournal existing = readJournalIfPresent(journalFile);
            if (existing != null) {
                return resumeBatch(existing, batchId, bucket, items, principal);
            }

            // ---- 整批校验（任何落盘动作之前；校验失败不留任何痕迹） ----
            Set<String> seenKeys = new HashSet<>();
            List<BatchPlan> plans = new ArrayList<>();
            Map<String, byte[]> contents = new HashMap<>();
            Set<String> newShas = new HashSet<>();
            long netNewBytes = 0;

            for (BatchItem item : items) {
                if (item == null || item.key() == null || item.request() == null) {
                    throw new StoreException(ErrorCode.BATCH_INVALID,
                            "batch item and its key/request must not be null");
                }
                String key = item.key();
                if (!seenKeys.add(key)) {
                    throw new StoreException(ErrorCode.BATCH_INVALID, "duplicate key within batch: " + key);
                }
                WriteRequest request = item.request();
                byte[] content = request.content();
                if (content == null || content.length == 0) {
                    throw new StoreException(ErrorCode.EMPTY_CONTENT,
                            "empty content is not allowed for key " + key);
                }
                String sha256 = sha256Hex(content);
                if (request.checksumSha256() != null && !request.checksumSha256().equalsIgnoreCase(sha256)) {
                    throw new StoreException(ErrorCode.CHECKSUM_MISMATCH,
                            "declared checksum " + request.checksumSha256() + " != actual " + sha256
                                    + " for key " + key);
                }
                Path objectDir = objectDir(bucket, key);
                long current = readHead(objectDir);
                if (request.expectedVersion() != null && request.expectedVersion() != current) {
                    throw new StoreException(ErrorCode.VERSION_CONFLICT,
                            "key " + key + ": expected version " + request.expectedVersion()
                                    + " but current is " + current);
                }
                ensureWritableByRetention(key, objectDir, current);

                long next = current + 1;
                RetentionPolicy policy = readRetention(objectDir);
                contents.put(sha256, content);
                plans.add(new BatchPlan(key, next, sha256, content.length,
                        request.expectedVersion(), policy.retentionUntil(), policy.complianceLock()));
                if (!Files.isRegularFile(blobsDir.resolve(sha256)) && newShas.add(sha256)) {
                    netNewBytes += content.length;
                }
            }

            // ---- 整批容量：只按净新增内容一次性核算 ----
            if (usedBytes.get() + netNewBytes > quotaBytes) {
                throw new StoreException(ErrorCode.QUOTA_EXCEEDED,
                        "batch quota exceeded: used=" + usedBytes.get() + " netNew=" + netNewBytes
                                + " quota=" + quotaBytes);
            }

            // ---- 写入净新增内容块（批内及与已有内容去重），全部先于元数据落盘 ----
            for (String sha : newShas) {
                ensureBlob(sha, contents.get(sha));
            }

            // test hook: crash after blobs are durable but before the journal exists
            Runnable preJournal = batchPreJournalHook;
            if (preJournal != null) {
                preJournal.run();
            }

            // ---- PREPARED 日志：崩溃恢复与幂等重试的依据 ----
            Instant now = Instant.now();
            List<JournalItem> journalItems = new ArrayList<>();
            for (int i = 0; i < plans.size(); i++) {
                BatchPlan p = plans.get(i);
                journalItems.add(new JournalItem(p.key(), p.version(), p.sha256(), p.size(),
                        p.expectedVersion(), items.get(i).request().checksumSha256(),
                        p.retentionUntil(), p.complianceLock()));
            }
            BatchJournal journal = new BatchJournal(batchId, bucket, principal, now, journalItems);
            writeAtomic(batchDir(batchId).resolve("journal.json.tmp"), journalFile, toJson(journal));
            fsyncDir(batchDir(batchId));

            return completeBatch(journal, now);
        } catch (IOException e) {
            throw new UncheckedIOException("batch commit failed: " + batchId, e);
        } finally {
            commitLock.unlock();
        }
    }

    @Override
    public VersionInfo put(String bucket, String key, WriteRequest request, String principal) {
        accessControl.require(bucket, principal, Role.WRITER);
        requireBucket(bucket);

        byte[] content = request.content();
        if (content == null || content.length == 0) {
            throw new StoreException(ErrorCode.EMPTY_CONTENT, "empty content is not allowed");
        }
        String sha256 = sha256Hex(content);
        if (request.checksumSha256() != null && !request.checksumSha256().equalsIgnoreCase(sha256)) {
            throw new StoreException(ErrorCode.CHECKSUM_MISMATCH,
                    "declared checksum " + request.checksumSha256() + " != actual " + sha256);
        }

        commitLock.lock();
        try {
            Path objectDir = objectDir(bucket, key);
            long current = readHead(objectDir);
            if (request.expectedVersion() != null && request.expectedVersion() != current) {
                throw new StoreException(ErrorCode.VERSION_CONFLICT,
                        "expected version " + request.expectedVersion() + " but current is " + current);
            }
            ensureWritableByRetention(key, objectDir, current);

            ensureBlob(sha256, content);

            long next = current + 1;
            RetentionPolicy retention = readRetention(objectDir);
            VersionInfo info = new VersionInfo(bucket, key, next, sha256, content.length,
                    Instant.now(), principal, retention.retentionUntil(), retention.complianceLock(), null);

            // 1) version metadata: tmp + fsync + atomic rename
            Path versionsDir = versionsDir(objectDir);
            Path meta = versionsDir.resolve("v-" + next + ".json");
            writeAtomic(versionsDir.resolve("v-" + next + ".json.tmp"), meta, toJson(info));

            // test hook: simulate crash with metadata durable but HEAD not updated
            Runnable hook = commitHook;
            if (hook != null) {
                hook.run();
            }

            // 2) HEAD: the commit point, tmp + fsync + atomic rename
            writeAtomic(objectDir.resolve("HEAD.tmp"), objectDir.resolve("HEAD"),
                    Long.toString(next).getBytes(StandardCharsets.UTF_8));
            fsyncDir(objectDir);
            return info;
        } catch (IOException e) {
            throw new UncheckedIOException("commit failed for " + bucket + "/" + key, e);
        } finally {
            commitLock.unlock();
        }
    }

    /**
     * Finish a prepared batch: write all version metadata, advance every HEAD
     * (batch versions stay invisible: readers additionally require the marker),
     * then atomically publish the commit marker — the single visibility point.
     */
    private BatchResult completeBatch(BatchJournal journal, Instant committedAt) throws IOException {
        Path marker = batchDir(journal.batchId()).resolve("committed");
        if (committedBatches.contains(journal.batchId()) || Files.isRegularFile(marker)) {
            return finishCommittedBatch(journal, committedAt);
        }

        // 1) 每个对象的新版本元数据（已存在则保留，崩溃重入安全）
        for (JournalItem it : journal.items()) {
            Path versionsDir = versionsDir(objectDir(journal.bucket(), it.key()));
            Path meta = versionsDir.resolve("v-" + it.version() + ".json");
            if (!Files.isRegularFile(meta)) {
                VersionInfo info = new VersionInfo(journal.bucket(), it.key(), it.version(),
                        it.sha256(), it.size(), committedAt, journal.principal(),
                        it.retentionUntil(), it.complianceLock(), journal.batchId());
                writeAtomic(versionsDir.resolve("v-" + it.version() + ".json.tmp"), meta, toJson(info));
            }
        }

        // 2) 推进每个对象的 HEAD；此时没有 marker，读者仍过滤掉这些批次版本
        for (JournalItem it : journal.items()) {
            Path objectDir = objectDir(journal.bucket(), it.key());
            if (it.version() > readHead(objectDir)) {
                writeAtomic(objectDir.resolve("HEAD.tmp"), objectDir.resolve("HEAD"),
                        Long.toString(it.version()).getBytes(StandardCharsets.UTF_8));
                fsyncDir(objectDir);
            }
        }

        // test hook: crash with HEADs advanced but the batch not yet visible
        Runnable pre = batchPreCommitHook;
        if (pre != null) {
            pre.run();
        }

        // 3) 原子发布：整批同时可见
        Path batchDir = batchDir(journal.batchId());
        writeAtomic(batchDir.resolve("committed.tmp"), marker,
                journal.batchId().getBytes(StandardCharsets.UTF_8));
        fsyncDir(batchDir);
        committedBatches.add(journal.batchId());

        // test hook: crash right after visibility
        Runnable post = batchPostCommitHook;
        if (post != null) {
            post.run();
        }

        return toBatchResult(journal, committedAt);
    }

    /** 已发布批次的幂等收尾：补齐可能缺失的元数据与 HEAD，返回稳定结果。 */
    private BatchResult finishCommittedBatch(BatchJournal journal, Instant committedAt) throws IOException {
        committedBatches.add(journal.batchId());
        for (JournalItem it : journal.items()) {
            Path versionsDir = versionsDir(objectDir(journal.bucket(), it.key()));
            Path meta = versionsDir.resolve("v-" + it.version() + ".json");
            if (!Files.isRegularFile(meta)) {
                VersionInfo info = new VersionInfo(journal.bucket(), it.key(), it.version(),
                        it.sha256(), it.size(), committedAt, journal.principal(),
                        it.retentionUntil(), it.complianceLock(), journal.batchId());
                writeAtomic(versionsDir.resolve("v-" + it.version() + ".json.tmp"), meta, toJson(info));
            }
            Path objectDir = objectDir(journal.bucket(), it.key());
            if (it.version() > readHead(objectDir)) {
                writeAtomic(objectDir.resolve("HEAD.tmp"), objectDir.resolve("HEAD"),
                        Long.toString(it.version()).getBytes(StandardCharsets.UTF_8));
                fsyncDir(objectDir);
            }
        }
        return toBatchResult(journal, committedAt);
    }

    private BatchResult toBatchResult(BatchJournal journal, Instant committedAt) {
        List<VersionInfo> infos = new ArrayList<>();
        for (JournalItem it : journal.items()) {
            infos.add(new VersionInfo(journal.bucket(), it.key(), it.version(),
                    it.sha256(), it.size(), committedAt, journal.principal(),
                    it.retentionUntil(), it.complianceLock(), journal.batchId()));
        }
        return new BatchResult(journal.batchId(), infos);
    }

    /** 同一 batchId 重试：身份一致则继续提交/返回稳定结果，否则整批拒绝。 */
    private BatchResult resumeBatch(BatchJournal journal, String batchId, String bucket,
                                    List<BatchItem> items, String principal) throws IOException {
        if (!journal.bucket().equals(bucket) || !journal.principal().equals(principal)
                || journal.items().size() != items.size()) {
            throw new StoreException(ErrorCode.BATCH_RETRY_CONFLICT,
                    "batch " + batchId + " was already prepared with a different identity");
        }
        for (int i = 0; i < items.size(); i++) {
            JournalItem planned = journal.items().get(i);
            BatchItem incoming = items.get(i);
            boolean sameKey = planned.key().equals(incoming.key());
            boolean sameSha = planned.sha256().equals(sha256Hex(incoming.request().content()));
            boolean sameExpected = Objects.equals(planned.expectedVersion(),
                    incoming.request().expectedVersion());
            boolean sameChecksum = Objects.equals(planned.checksumSha256(),
                    incoming.request().checksumSha256());
            if (!sameKey || !sameSha || !sameExpected || !sameChecksum) {
                throw new StoreException(ErrorCode.BATCH_RETRY_CONFLICT,
                        "batch " + batchId + " retry item " + i + " differs from the first submission");
            }
        }
        return completeBatch(journal, journal.committedAt());
    }

    /**
     * 写时保留闸门：对象当前 HEAD 版本处于保留期内或被合规锁定时，
     * 覆盖（产生新版本）被拒绝；新版本仍照常继承当前保留策略。
     */
    private void ensureWritableByRetention(String key, Path objectDir, long current) {
        if (current <= 0) {
            return;
        }
        Path headMeta = versionsDir(objectDir).resolve("v-" + current + ".json");
        if (!Files.isRegularFile(headMeta)) {
            return;
        }
        VersionInfo head = VersionInfoCodec.read(headMeta);
        if (head.complianceLock()) {
            throw new StoreException(ErrorCode.COMPLIANCE_LOCKED,
                    "object " + key + " is compliance-locked at version " + current
                            + "; overwrite rejected");
        }
        if (head.retentionUntil() != null && Instant.now().isBefore(head.retentionUntil())) {
            throw new StoreException(ErrorCode.RETENTION_ACTIVE,
                    "object " + key + " is retained until " + head.retentionUntil()
                            + "; overwrite rejected");
        }
    }

    /** 校验阶段的每对象计划（仅进程内使用）。 */
    private record BatchPlan(String key, long version, String sha256, long size,
                             Long expectedVersion, Instant retentionUntil, boolean complianceLock) {
    }

    /** 持久化批次日志中的单项（携带用于重试身份比对的版本条件与校验和）。 */
    record JournalItem(String key, long version, String sha256, long size, Long expectedVersion,
                       String checksumSha256, Instant retentionUntil, boolean complianceLock) {
    }

    /** 持久化批次日志（存在即 PREPARED；committed 标记决定可见性）。 */
    record BatchJournal(String batchId, String bucket, String principal,
                        Instant committedAt, List<JournalItem> items) {
    }

    // ------------------------------------------------------------------ get

    @Override
    public byte[] get(String bucket, String key, String principal) {
        accessControl.require(bucket, principal, Role.READER);
        return get(bucket, key, latestCommittedVersion(bucket, key), principal);
    }

    @Override
    public byte[] get(String bucket, String key, long version, String principal) {
        accessControl.require(bucket, principal, Role.READER);
        Path objectDir = objectDir(bucket, key);
        Path meta = versionsDir(objectDir).resolve("v-" + version + ".json");
        long head = readHead(objectDir);
        if (!Files.isRegularFile(meta) || version > head) {
            throw new StoreException(ErrorCode.OBJECT_NOT_FOUND,
                    "no committed version " + version + " of " + bucket + "/" + key);
        }
        VersionInfo info = VersionInfoCodec.read(meta);
        if (!isVisible(info)) {
            // 批次版本的提交标记尚不存在：整批尚未发布
            throw new StoreException(ErrorCode.OBJECT_NOT_FOUND,
                    "no committed version " + version + " of " + bucket + "/" + key);
        }
        Path blob = blobsDir.resolve(info.sha256());
        if (!Files.isRegularFile(blob)) {
            throw new StoreException(ErrorCode.OBJECT_NOT_FOUND,
                    "content blob " + info.sha256() + " is missing");
        }
        try {
            byte[] content = Files.readAllBytes(blob);
            if (!sha256Hex(content).equals(info.sha256())) {
                throw new StoreException(ErrorCode.CHECKSUM_MISMATCH,
                        "blob " + info.sha256() + " failed integrity check on read");
            }
            return content;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read blob " + blob, e);
        }
    }

    @Override
    public List<VersionInfo> listVersions(String bucket, String key, String principal) {
        accessControl.require(bucket, principal, Role.READER);
        Path objectDir = objectDir(bucket, key);
        long head = readHead(objectDir);
        List<VersionInfo> result = new ArrayList<>();
        for (Path meta : listVersionFiles(objectDir)) {
            long v = VersionInfoCodec.versionOf(meta);
            if (v <= head) { // only expose committed versions
                VersionInfo info = VersionInfoCodec.read(meta);
                if (isVisible(info)) {
                    result.add(info); // 未发布批次的版本整体不可见
                }
            }
        }
        result.sort(Comparator.comparingLong(VersionInfo::version));
        if (result.isEmpty()) {
            throw new StoreException(ErrorCode.OBJECT_NOT_FOUND,
                    "object " + bucket + "/" + key + " not found");
        }
        return result;
    }

    @Override
    public long headVersion(String bucket, String key, String principal) {
        accessControl.require(bucket, principal, Role.READER);
        return latestCommittedVersion(bucket, key);
    }

    // ------------------------------------------------------------- retention

    @Override
    public void setRetention(String bucket, String key, RetentionPolicy policy, String principal) {
        accessControl.require(bucket, principal, Role.WRITER);
        requireBucket(bucket);
        commitLock.lock();
        try {
            Path objectDir = objectDir(bucket, key);
            Files.createDirectories(objectDir);
            writeAtomic(objectDir.resolve("retention.json.tmp"),
                    objectDir.resolve("retention.json"), toJson(policy));
            fsyncDir(objectDir);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to set retention on " + bucket + "/" + key, e);
        } finally {
            commitLock.unlock();
        }
    }

    // ---------------------------------------------------------------- delete

    @Override
    public void deleteVersion(String bucket, String key, long version, String principal) {
        accessControl.require(bucket, principal, Role.WRITER);
        commitLock.lock();
        try {
            Path objectDir = objectDir(bucket, key);
            Path meta = versionsDir(objectDir).resolve("v-" + version + ".json");
            if (!Files.isRegularFile(meta) || version > readHead(objectDir)) {
                throw new StoreException(ErrorCode.OBJECT_NOT_FOUND,
                        "no committed version " + version + " of " + bucket + "/" + key);
            }
            VersionInfo info = VersionInfoCodec.read(meta);
            if (!isVisible(info)) {
                throw new StoreException(ErrorCode.OBJECT_NOT_FOUND,
                        "no committed version " + version + " of " + bucket + "/" + key);
            }
            if (info.complianceLock()) {
                throw new StoreException(ErrorCode.COMPLIANCE_LOCKED,
                        "version " + version + " of " + bucket + "/" + key + " is compliance-locked");
            }
            if (info.retentionUntil() != null && Instant.now().isBefore(info.retentionUntil())) {
                throw new StoreException(ErrorCode.RETENTION_ACTIVE,
                        "version " + version + " of " + bucket + "/" + key
                                + " is retained until " + info.retentionUntil());
            }
            Files.delete(meta);
            fsyncDir(versionsDir(objectDir));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to delete version " + version
                    + " of " + bucket + "/" + key, e);
        } finally {
            commitLock.unlock();
        }
    }

    // ------------------------------------------------------------------- GC

    @Override
    public long garbageCollect(String principal) {
        commitLock.lock();
        try {
            Set<String> referenced = new HashSet<>();
            collectReferencedSha256(bucketsDir, referenced);
            long reclaimed = 0;
            synchronized (blobLock) {
                try (DirectoryStream<Path> blobs = Files.newDirectoryStream(blobsDir)) {
                    for (Path blob : blobs) {
                        String name = blob.getFileName().toString();
                        if (name.endsWith(".tmp")) {
                            Files.deleteIfExists(blob);
                            continue;
                        }
                        if (!referenced.contains(name)) {
                            long size = Files.size(blob);
                            Files.delete(blob);
                            usedBytes.addAndGet(-size);
                            reclaimed += size;
                        }
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException("garbage collection failed", e);
                }
            }
            fsyncDirQuiet(blobsDir);
            return reclaimed;
        } finally {
            commitLock.unlock();
        }
    }

    // ------------------------------------------------------------- recovery

    /**
     * Crash recovery at construction:
     * 1) delete every *.tmp under the store root;
     * 2) roll forward every durable batch journal to completion (metadata -> HEADs
     *    -> commit marker), so a crash mid-batch ends in all-committed;
     * 3) delete dangling version files beyond each object's HEAD — they belong to
     *    attempts that never reached a durable journal and count as not committed.
     */
    private void recoverUncommittedState() {
        try {
            deleteTempFiles(batchesDir);
            deleteTempFiles(blobsDir);
            if (Files.isDirectory(bucketsDir)) {
                try (var bucketStream = Files.list(bucketsDir)) {
                    for (Path bucket : bucketStream.filter(Files::isDirectory).toList()) {
                        Path objectsDir = bucket.resolve("objects");
                        if (!Files.isDirectory(objectsDir)) {
                            continue;
                        }
                        try (var objectStream = Files.list(objectsDir)) {
                            for (Path objectDir : objectStream.filter(Files::isDirectory).toList()) {
                                deleteTempFiles(objectDir);
                            }
                        }
                    }
                }
            }
            rollForwardPreparedBatches();
            if (Files.isDirectory(bucketsDir)) {
                try (var bucketStream = Files.list(bucketsDir)) {
                    for (Path bucket : bucketStream.filter(Files::isDirectory).toList()) {
                        Path objectsDir = bucket.resolve("objects");
                        if (!Files.isDirectory(objectsDir)) {
                            continue;
                        }
                        try (var objectStream = Files.list(objectsDir)) {
                            for (Path objectDir : objectStream.filter(Files::isDirectory).toList()) {
                                deleteDanglingVersions(objectDir);
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("recovery failed", e);
        }
    }

    /** 前滚所有已落盘日志的批次：结果统一为整批提交成功、完整可读。 */
    private void rollForwardPreparedBatches() throws IOException {
        if (!Files.isDirectory(batchesDir)) {
            return;
        }
        try (var stream = Files.list(batchesDir)) {
            for (Path batchDir : stream.filter(Files::isDirectory).toList()) {
                Path journalFile = batchDir.resolve("journal.json");
                if (!Files.isRegularFile(journalFile)) {
                    continue;
                }
                BatchJournal journal;
                try {
                    journal = JSON.readValue(journalFile.toFile(), BatchJournal.class);
                } catch (IOException e) {
                    throw new UncheckedIOException("failed to read batch journal " + journalFile, e);
                }
                if (Files.isRegularFile(batchDir.resolve("committed"))) {
                    committedBatches.add(journal.batchId());
                }
                completeBatch(journal, journal.committedAt());
            }
        }
    }

    private void deleteDanglingVersions(Path objectDir) throws IOException {
        long head = readHead(objectDir);
        Path versionsDir = versionsDir(objectDir);
        if (!Files.isDirectory(versionsDir)) {
            return;
        }
        try (var stream = Files.list(versionsDir)) {
            for (Path meta : stream.filter(VersionInfoCodec::isVersionFile).toList()) {
                if (VersionInfoCodec.versionOf(meta) > head) {
                    Files.delete(meta); // dangling, no durable journal -> never committed
                }
            }
        }
    }

    private static void deleteTempFiles(Path dir) throws IOException {
        if (!Files.isDirectory(dir)) {
            return;
        }
        try (var stream = Files.walk(dir)) {
            for (Path p : stream.filter(Files::isRegularFile)
                    .filter(f -> f.getFileName().toString().endsWith(".tmp")).toList()) {
                Files.deleteIfExists(p);
            }
        }
    }

    // --------------------------------------------------------------- helpers

    private void ensureBlob(String sha256, byte[] content) throws IOException {
        Path blob = blobsDir.resolve(sha256);
        if (Files.isRegularFile(blob)) {
            return; // dedup: existing content never occupies storage twice
        }
        synchronized (blobLock) {
            if (Files.isRegularFile(blob)) {
                return;
            }
            if (usedBytes.get() + content.length > quotaBytes) {
                throw new StoreException(ErrorCode.QUOTA_EXCEEDED,
                        "quota exceeded: used=" + usedBytes.get() + " new=" + content.length
                                + " quota=" + quotaBytes);
            }
            writeAtomic(blobsDir.resolve(sha256 + ".tmp"), blob, content);
            fsyncDir(blobsDir);
            usedBytes.addAndGet(content.length);
        }
    }

    private long latestCommittedVersion(String bucket, String key) {
        Path objectDir = objectDir(bucket, key);
        long head = readHead(objectDir);
        long latest = -1;
        for (Path meta : listVersionFiles(objectDir)) {
            long v = VersionInfoCodec.versionOf(meta);
            if (v <= head && v > latest) {
                VersionInfo info = VersionInfoCodec.read(meta);
                if (isVisible(info)) {
                    latest = v; // 跳过未发布批次的版本
                }
            }
        }
        if (latest < 0) {
            throw new StoreException(ErrorCode.OBJECT_NOT_FOUND,
                    "object " + bucket + "/" + key + " not found");
        }
        return latest;
    }

    /**
     * 版本可见性：单对象写入（batchId == null）随 HEAD 提交即可见；
     * 批次版本仅当对应批次的 committed 标记存在时可见——这是整批的原子可见点。
     */
    private boolean isVisible(VersionInfo info) {
        String batchId = info.batchId();
        if (batchId == null) {
            return true;
        }
        if (committedBatches.contains(batchId)) {
            return true;
        }
        boolean marked = Files.isRegularFile(batchDir(batchId).resolve("committed"));
        if (marked) {
            committedBatches.add(batchId);
        }
        return marked;
    }

    private BatchJournal readJournalIfPresent(Path journalFile) {
        if (!Files.isRegularFile(journalFile)) {
            return null;
        }
        try {
            return JSON.readValue(journalFile.toFile(), BatchJournal.class);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read batch journal " + journalFile, e);
        }
    }

    private Path batchDir(String batchId) {
        return batchesDir.resolve(encode(batchId));
    }

    private List<Path> listVersionFiles(Path objectDir) {
        Path versionsDir = versionsDir(objectDir);
        List<Path> result = new ArrayList<>();
        if (!Files.isDirectory(versionsDir)) {
            return result;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(versionsDir, "v-*.json")) {
            for (Path p : stream) {
                if (VersionInfoCodec.isVersionFile(p)) {
                    result.add(p);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to list versions in " + versionsDir, e);
        }
        return result;
    }

    private void collectReferencedSha256(Path dir, Set<String> out) {
        if (!Files.isDirectory(dir)) {
            return;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path p : stream) {
                if (Files.isDirectory(p)) {
                    collectReferencedSha256(p, out);
                } else if (VersionInfoCodec.isVersionFile(p)) {
                    out.add(VersionInfoCodec.read(p).sha256());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to scan " + dir, e);
        }
    }

    private long readHead(Path objectDir) {
        Path head = objectDir.resolve("HEAD");
        if (!Files.isRegularFile(head)) {
            return 0L;
        }
        try {
            return Long.parseLong(Files.readString(head).trim());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read HEAD of " + objectDir, e);
        }
    }

    private RetentionPolicy readRetention(Path objectDir) {
        Path file = objectDir.resolve("retention.json");
        if (!Files.isRegularFile(file)) {
            return RetentionPolicy.NONE;
        }
        try {
            return JSON.readValue(file.toFile(), RetentionPolicy.class);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read retention of " + objectDir, e);
        }
    }

    private void requireBucket(String bucket) {
        if (!Files.isDirectory(bucketDir(bucket))) {
            throw new StoreException(ErrorCode.OBJECT_NOT_FOUND, "bucket '" + bucket + "' not found");
        }
    }

    private Path bucketDir(String bucket) {
        return bucketsDir.resolve(encode(bucket));
    }

    private Path objectDir(String bucket, String key) {
        return bucketDir(bucket).resolve("objects").resolve(encode(key));
    }

    private static Path versionsDir(Path objectDir) {
        return objectDir.resolve("versions");
    }

    /** Percent-encode keys so any UTF-8 key maps to a safe, reversible path segment. */
    static String encode(String key) {
        StringBuilder sb = new StringBuilder();
        for (byte b : key.getBytes(StandardCharsets.UTF_8)) {
            int c = b & 0xff;
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                    || c == '.' || c == '-' || c == '_') {
                sb.append((char) c);
            } else {
                sb.append('%').append(Character.forDigit((c >> 4) & 0xf, 16))
                        .append(Character.forDigit(c & 0xf, 16));
            }
        }
        return sb.toString();
    }

    private static String sha256Hex(byte[] content) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] toJson(Object value) {
        try {
            return JSON.writeValueAsBytes(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Write tmp file, fsync it, then atomically rename onto the target. */
    private static void writeAtomic(Path tmp, Path target, byte[] content) throws IOException {
        Files.createDirectories(target.getParent());
        try (FileChannel channel = FileChannel.open(tmp,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            channel.write(ByteBuffer.wrap(content));
            channel.force(true);
        }
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    private static void fsyncDir(Path dir) throws IOException {
        try (FileChannel channel = FileChannel.open(dir, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private static void fsyncDirQuiet(Path dir) {
        try {
            fsyncDir(dir);
        } catch (IOException ignored) {
            // best effort
        }
    }

    private long sumBlobBytes() {
        long sum = 0;
        if (!Files.isDirectory(blobsDir)) {
            return 0;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(blobsDir)) {
            for (Path blob : stream) {
                if (Files.isRegularFile(blob) && !blob.getFileName().toString().endsWith(".tmp")) {
                    sum += Files.size(blob);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to account blob bytes", e);
        }
        return sum;
    }
}
