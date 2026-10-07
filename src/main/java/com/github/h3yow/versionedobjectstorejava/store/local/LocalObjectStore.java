
package com.github.h3yow.versionedobjectstorejava.store.local;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.h3yow.versionedobjectstorejava.store.ErrorCode;
import com.github.h3yow.versionedobjectstorejava.store.ObjectStore;
import com.github.h3yow.versionedobjectstorejava.store.StoreException;
import com.github.h3yow.versionedobjectstorejava.store.acl.LocalAccessControl;
import com.github.h3yow.versionedobjectstorejava.store.acl.Role;
import com.github.h3yow.versionedobjectstorejava.store.model.BatchCommit;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Local-disk only implementation of {@link ObjectStore}.
 *
 * Disk layout:
 *   root/blobs/<sha256>                       content-addressed immutable blobs (dedup)
 *   root/buckets/<bucket>/objects/<key>/versions/v-<n>.json   immutable version metadata
 *   root/buckets/<bucket>/objects/<key>/HEAD  last issued version number (commit point)
 *   root/buckets/<bucket>/objects/<key>/retention.json        retention policy
 *
 * Commit protocol (under a per-object lock): validate -> write new blob
 * (tmp + fsync + atomic rename, quota accounted) -> write version metadata
 * tmp + fsync -> atomic rename to v-<n>.json -> atomic HEAD update -> fsync dir.
 * HEAD is the commit point: readers only accept version files <= HEAD, so no
 * half-committed state is ever visible.
 *
 * Crash recovery (at construction): delete all *.tmp files and dangling version
 * files with version > HEAD; blobs not referenced by any committed version are
 * reclaimed by garbageCollect.
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

    /** Per-object commit locks, map key = bucket + ' ' + key. */
    private final Map<String, ReentrantLock> objectLocks = new ConcurrentHashMap<>();
    /** Global lock guarding blob creation and quota accounting. */
    private final Object blobLock = new Object();
    /** Serializes batch commits so identical batchIds cannot race each other. */
    private final ReentrantLock batchLock = new ReentrantLock();
    /**
     * Visibility lock: readers hold the read lock, a batch commit holds the write
     * lock while flipping all HEADs, so the whole batch becomes visible in one
     * atomic transition and no read observes a half-applied batch.
     */
    private final ReentrantReadWriteLock visibilityLock = new ReentrantReadWriteLock();
    private final AtomicLong usedBytes;

    /** Test hook: fired after version metadata is durable but before HEAD update. */
    private volatile Runnable commitHook = null;
    /** Test hook: fired after the batch commit record is durable but before HEAD flips. */
    private volatile Runnable batchCommitHook = null;
    /** Test hook: fired after batch metadata is durable, before the commit record. */
    private volatile Runnable batchPrepareHook = null;

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

    /** Test only: inject a hook after the batch commit record is durable, before HEAD flips. */
    public void setBatchCommitHookForTesting(Runnable hook) {
        this.batchCommitHook = hook;
    }

    /** Test only: inject a hook after batch metadata is durable, before the commit record. */
    public void setBatchPrepareHookForTesting(Runnable hook) {
        this.batchPrepareHook = hook;
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

        ReentrantLock lock = objectLock(bucket, key);
        lock.lock();
        try {
            Path objectDir = objectDir(bucket, key);
            long current = readHead(objectDir);
            if (request.expectedVersion() != null && request.expectedVersion() != current) {
                throw new StoreException(ErrorCode.VERSION_CONFLICT,
                        "expected version " + request.expectedVersion() + " but current is " + current);
            }

            ensureBlob(sha256, content);

            long next = current + 1;
            RetentionPolicy retention = readRetention(objectDir);
            VersionInfo info = new VersionInfo(bucket, key, next, sha256, content.length,
                    Instant.now(), principal, retention.retentionUntil(), retention.complianceLock());

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
            lock.unlock();
        }
    }

    // ------------------------------------------------------------- batch

    /**
     * Batch commit protocol (batchLock -> per-object locks in sorted order ->
     * blobLock -> visibility write lock):
     *   0) idempotency: a durable commit record for batchId means the batch is
     *      already committed; replay the stored result without new versions/quota.
     *   1) validate everything (ACL, structure, checksum, expectedVersion,
     *      retention guard) before writing a single byte;
     *   2) under blobLock: net-new bytes only (dedup within batch and against
     *      existing blobs); quota check rejects the whole batch before any write;
     *   3) write new blobs, then all version metadata files (tmp+fsync+rename);
     *   4) atomically write batches/<batchId>.json — THE commit point;
     *   5) under the visibility write lock flip every object's HEAD.
     * Crash before 4: nothing visible, dangling metadata cleaned on restart,
     * unreferenced blobs reclaimed by GC. Crash after 4: recovery replays the
     * HEAD flips from the commit record.
     */
    @Override
    public BatchResult commitBatch(String bucket, BatchCommit batch, String principal) {
        accessControl.require(bucket, principal, Role.WRITER);
        requireBucket(bucket);
        validateBatchStructure(batch);
        String fingerprint = fingerprint(batch);

        batchLock.lock();
        try {
            Path recordFile = batchesDir.resolve(encode(batch.batchId()) + ".json");
            BatchCommitRecord existing = readBatchRecord(recordFile);
            if (existing != null) {
                if (!existing.fingerprint().equals(fingerprint)) {
                    throw new StoreException(ErrorCode.BATCH_ID_REUSED,
                            "batch id '" + batch.batchId() + "' was already used by a different commit");
                }
                return new BatchResult(batch.batchId(), existing.versions(), true);
            }

            // per-object locks in sorted key order: serialize with single puts
            List<BatchItem> items = batch.items().stream()
                    .sorted(Comparator.comparing(BatchItem::key)).toList();
            List<ReentrantLock> locks = items.stream()
                    .map(i -> objectLock(bucket, i.key())).distinct().toList();
            locks.forEach(ReentrantLock::lock);
            try {
                List<VersionInfo> prepared = prepareBatch(bucket, items, principal);

                // test hook: simulate crash with metadata durable but no commit record
                Runnable prepareHook = batchPrepareHook;
                if (prepareHook != null) {
                    prepareHook.run();
                }

                // result order follows the caller's item order
                Map<String, VersionInfo> byKey = new HashMap<>();
                for (VersionInfo v : prepared) {
                    byKey.put(v.key(), v);
                }
                List<VersionInfo> versions = batch.items().stream()
                        .map(i -> byKey.get(i.key())).toList();
                BatchCommitRecord record = new BatchCommitRecord(
                        batch.batchId(), fingerprint, bucket, principal,
                        versions.get(0).committedAt(), versions);

                // 4) commit point: durable commit record
                writeAtomic(batchesDir.resolve(encode(batch.batchId()) + ".json.tmp"),
                        recordFile, toJson(record));
                fsyncDir(batchesDir);

                Runnable hook = batchCommitHook;
                if (hook != null) {
                    hook.run();
                }

                // 5) atomic visibility transition: flip all HEADs at once
                visibilityLock.writeLock().lock();
                try {
                    for (VersionInfo info : versions) {
                        Path objectDir = objectDir(bucket, info.key());
                        writeAtomic(objectDir.resolve("HEAD.tmp"), objectDir.resolve("HEAD"),
                                Long.toString(info.version()).getBytes(StandardCharsets.UTF_8));
                        fsyncDir(objectDir);
                    }
                } finally {
                    visibilityLock.writeLock().unlock();
                }
                return new BatchResult(batch.batchId(), versions, false);
            } finally {
                locks.forEach(ReentrantLock::unlock);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("batch commit failed for id " + batch.batchId(), e);
        } finally {
            batchLock.unlock();
        }
    }

    /** Structural validation: non-blank id, non-empty items, no duplicate keys. */
    private static void validateBatchStructure(BatchCommit batch) {
        if (batch.batchId() == null || batch.batchId().isBlank()) {
            throw new StoreException(ErrorCode.INVALID_BATCH, "batch id must not be blank");
        }
        if (batch.items() == null || batch.items().isEmpty()) {
            throw new StoreException(ErrorCode.INVALID_BATCH, "batch must contain at least one item");
        }
        Set<String> keys = new HashSet<>();
        for (BatchItem item : batch.items()) {
            if (!keys.add(item.key())) {
                throw new StoreException(ErrorCode.INVALID_BATCH,
                        "duplicate key '" + item.key() + "' in batch " + batch.batchId());
            }
        }
    }

    /** Canonical fingerprint of the batch content, to detect id reuse with different content. */
    private static String fingerprint(BatchCommit batch) {
        StringBuilder sb = new StringBuilder();
        batch.items().stream().sorted(Comparator.comparing(BatchItem::key)).forEach(item -> {
            WriteRequest r = item.request();
            sb.append(item.key()).append(' ')
                    .append(r.expectedVersion() == null ? "*" : r.expectedVersion()).append(' ')
                    .append(sha256Hex(r.content() == null ? new byte[0] : r.content())).append('\n');
        });
        return sha256Hex(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Validate all items, write new blobs (quota accounted, all-or-nothing under
     * blobLock) and all version metadata files. Must be called holding the
     * per-object locks of all items.
     */
    private List<VersionInfo> prepareBatch(String bucket, List<BatchItem> items, String principal)
            throws IOException {
        // 1) validate everything before writing a single byte
        Map<String, byte[]> newContents = new LinkedHashMap<>();
        for (BatchItem item : items) {
            WriteRequest request = item.request();
            byte[] content = request.content();
            if (content == null || content.length == 0) {
                throw new StoreException(ErrorCode.EMPTY_CONTENT, "empty content is not allowed");
            }
            String sha256 = sha256Hex(content);
            if (request.checksumSha256() != null && !request.checksumSha256().equalsIgnoreCase(sha256)) {
                throw new StoreException(ErrorCode.CHECKSUM_MISMATCH,
                        "declared checksum " + request.checksumSha256() + " != actual " + sha256);
            }
            Path objectDir = objectDir(bucket, item.key());
            long current = readHead(objectDir);
            if (request.expectedVersion() != null && request.expectedVersion() != current) {
                throw new StoreException(ErrorCode.VERSION_CONFLICT,
                        "object " + bucket + "/" + item.key() + ": expected version "
                                + request.expectedVersion() + " but current is " + current);
            }
            RetentionPolicy retention = readRetention(objectDir);
            if (retention.complianceLock()) {
                throw new StoreException(ErrorCode.COMPLIANCE_LOCKED,
                        "object " + bucket + "/" + item.key() + " is compliance-locked");
            }
            if (retention.retentionUntil() != null && Instant.now().isBefore(retention.retentionUntil())) {
                throw new StoreException(ErrorCode.RETENTION_ACTIVE,
                        "object " + bucket + "/" + item.key()
                                + " is retained until " + retention.retentionUntil());
            }
            newContents.putIfAbsent(sha256, content); // dedup within the batch
        }

        // 2) write net-new blobs atomically w.r.t. quota (all-or-nothing)
        ensureBlobs(newContents);

        // 3) version metadata files (still invisible: HEAD not yet flipped)
        List<VersionInfo> versions = new ArrayList<>();
        Instant now = Instant.now();
        for (BatchItem item : items) {
            Path objectDir = objectDir(bucket, item.key());
            long next = readHead(objectDir) + 1;
            byte[] content = item.request().content();
            RetentionPolicy retention = readRetention(objectDir);
            VersionInfo info = new VersionInfo(bucket, item.key(), next, sha256Hex(content),
                    content.length, now, principal, retention.retentionUntil(), retention.complianceLock());
            Path versionsDir = versionsDir(objectDir);
            writeAtomic(versionsDir.resolve("v-" + next + ".json.tmp"),
                    versionsDir.resolve("v-" + next + ".json"), toJson(info));
            versions.add(info);
        }
        return versions;
    }

    /**
     * Write all not-yet-present blobs under blobLock: quota is checked against
     * the net-new bytes only, and the whole set is accounted atomically, so a
     * quota rejection leaves no residual usage.
     */
    private void ensureBlobs(Map<String, byte[]> newContents) throws IOException {
        synchronized (blobLock) {
            Map<String, byte[]> toWrite = new LinkedHashMap<>();
            long netNew = 0;
            for (Map.Entry<String, byte[]> e : newContents.entrySet()) {
                if (!Files.isRegularFile(blobsDir.resolve(e.getKey()))) {
                    toWrite.put(e.getKey(), e.getValue());
                    netNew += e.getValue().length;
                }
            }
            if (usedBytes.get() + netNew > quotaBytes) {
                throw new StoreException(ErrorCode.QUOTA_EXCEEDED,
                        "quota exceeded: used=" + usedBytes.get() + " netNew=" + netNew
                                + " quota=" + quotaBytes);
            }
            for (Map.Entry<String, byte[]> e : toWrite.entrySet()) {
                writeAtomic(blobsDir.resolve(e.getKey() + ".tmp"),
                        blobsDir.resolve(e.getKey()), e.getValue());
            }
            fsyncDir(blobsDir);
            usedBytes.addAndGet(netNew);
        }
    }

    private BatchCommitRecord readBatchRecord(Path recordFile) {
        if (!Files.isRegularFile(recordFile)) {
            return null;
        }
        try {
            return JSON.readValue(recordFile.toFile(), BatchCommitRecord.class);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read batch record " + recordFile, e);
        }
    }

    // ------------------------------------------------------------------ get

    @Override
    public byte[] get(String bucket, String key, String principal) {
        accessControl.require(bucket, principal, Role.READER);
        visibilityLock.readLock().lock();
        try {
            return get(bucket, key, latestCommittedVersion(bucket, key), principal);
        } finally {
            visibilityLock.readLock().unlock();
        }
    }

    @Override
    public byte[] get(String bucket, String key, long version, String principal) {
        accessControl.require(bucket, principal, Role.READER);
        visibilityLock.readLock().lock();
        try {
            return doGet(bucket, key, version);
        } finally {
            visibilityLock.readLock().unlock();
        }
    }

    private byte[] doGet(String bucket, String key, long version) {
        Path objectDir = objectDir(bucket, key);
        Path meta = versionsDir(objectDir).resolve("v-" + version + ".json");
        long head = readHead(objectDir);
        if (!Files.isRegularFile(meta) || version > head) {
            throw new StoreException(ErrorCode.OBJECT_NOT_FOUND,
                    "no committed version " + version + " of " + bucket + "/" + key);
        }
        VersionInfo info = VersionInfoCodec.read(meta);
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
        visibilityLock.readLock().lock();
        try {
            return doListVersions(bucket, key);
        } finally {
            visibilityLock.readLock().unlock();
        }
    }

    private List<VersionInfo> doListVersions(String bucket, String key) {
        Path objectDir = objectDir(bucket, key);
        long head = readHead(objectDir);
        List<VersionInfo> result = new ArrayList<>();
        for (Path meta : listVersionFiles(objectDir)) {
            long v = VersionInfoCodec.versionOf(meta);
            if (v <= head) { // only expose committed versions
                result.add(VersionInfoCodec.read(meta));
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
        visibilityLock.readLock().lock();
        try {
            return latestCommittedVersion(bucket, key);
        } finally {
            visibilityLock.readLock().unlock();
        }
    }

    // ------------------------------------------------------------- retention

    @Override
    public void setRetention(String bucket, String key, RetentionPolicy policy, String principal) {
        accessControl.require(bucket, principal, Role.WRITER);
        requireBucket(bucket);
        ReentrantLock lock = objectLock(bucket, key);
        lock.lock();
        try {
            Path objectDir = objectDir(bucket, key);
            Files.createDirectories(objectDir);
            writeAtomic(objectDir.resolve("retention.json.tmp"),
                    objectDir.resolve("retention.json"), toJson(policy));
            fsyncDir(objectDir);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to set retention on " + bucket + "/" + key, e);
        } finally {
            lock.unlock();
        }
    }

    // ---------------------------------------------------------------- delete

    @Override
    public void deleteVersion(String bucket, String key, long version, String principal) {
        accessControl.require(bucket, principal, Role.WRITER);
        ReentrantLock lock = objectLock(bucket, key);
        lock.lock();
        try {
            Path objectDir = objectDir(bucket, key);
            Path meta = versionsDir(objectDir).resolve("v-" + version + ".json");
            if (!Files.isRegularFile(meta) || version > readHead(objectDir)) {
                throw new StoreException(ErrorCode.OBJECT_NOT_FOUND,
                        "no committed version " + version + " of " + bucket + "/" + key);
            }
            VersionInfo info = VersionInfoCodec.read(meta);
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
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------- GC

    @Override
    public long garbageCollect(String principal) {
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
    }

    // ------------------------------------------------------------- recovery

    /**
     * Crash recovery, in order:
     *   1) delete all *.tmp files (objects, blobs, batches);
     *   2) replay durable batch commit records: flip any HEAD that the commit
     *      point had already decided but the crash left behind;
     *   3) delete dangling version files with version > HEAD (never committed).
     */
    private void recoverUncommittedState() {
        if (!Files.isDirectory(bucketsDir)) {
            return;
        }
        try {
            deleteTempFiles();
            replayCommittedBatches();
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
        } catch (IOException e) {
            throw new UncheckedIOException("recovery failed", e);
        }
    }

    private void deleteTempFiles() throws IOException {
        List<Path> dirs = new ArrayList<>();
        dirs.add(blobsDir);
        dirs.add(batchesDir);
        if (Files.isDirectory(bucketsDir)) {
            try (var bucketStream = Files.list(bucketsDir)) {
                for (Path bucket : bucketStream.filter(Files::isDirectory).toList()) {
                    Path objectsDir = bucket.resolve("objects");
                    if (!Files.isDirectory(objectsDir)) {
                        continue;
                    }
                    try (var objectStream = Files.list(objectsDir)) {
                        for (Path objectDir : objectStream.filter(Files::isDirectory).toList()) {
                            dirs.add(objectDir);
                            dirs.add(versionsDir(objectDir));
                        }
                    }
                }
            }
        }
        for (Path dir : dirs) {
            if (!Files.isDirectory(dir)) {
                continue;
            }
            try (var stream = Files.list(dir)) {
                for (Path f : stream.filter(p -> p.getFileName().toString().endsWith(".tmp")).toList()) {
                    Files.deleteIfExists(f);
                }
            }
        }
    }

    /**
     * Replay every durable batch commit record: for each recorded version, if the
     * object's HEAD is still behind, flip it forward. Idempotent — a HEAD already
     * at or beyond the recorded version is left untouched.
     */
    private void replayCommittedBatches() throws IOException {
        if (!Files.isDirectory(batchesDir)) {
            return;
        }
        try (var stream = Files.list(batchesDir)) {
            for (Path recordFile : stream.filter(p -> p.getFileName().toString().endsWith(".json"))
                    .toList()) {
                BatchCommitRecord record = readBatchRecord(recordFile);
                for (VersionInfo info : record.versions()) {
                    Path objectDir = objectDir(record.bucket(), info.key());
                    if (readHead(objectDir) < info.version()) {
                        writeAtomic(objectDir.resolve("HEAD.tmp"), objectDir.resolve("HEAD"),
                                Long.toString(info.version()).getBytes(StandardCharsets.UTF_8));
                        fsyncDir(objectDir);
                    }
                }
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
                    Files.delete(meta); // dangling, never committed
                }
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
                latest = v;
            }
        }
        if (latest < 0) {
            throw new StoreException(ErrorCode.OBJECT_NOT_FOUND,
                    "object " + bucket + "/" + key + " not found");
        }
        return latest;
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

    private ReentrantLock objectLock(String bucket, String key) {
        return objectLocks.computeIfAbsent(bucket + ' ' + key, k -> new ReentrantLock());
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
