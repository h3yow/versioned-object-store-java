package com.github.h3yow.versionedobjectstorejava.store;

import com.github.h3yow.versionedobjectstorejava.store.acl.LocalAccessControl;
import com.github.h3yow.versionedobjectstorejava.store.acl.Role;
import com.github.h3yow.versionedobjectstorejava.store.local.LocalObjectStore;
import com.github.h3yow.versionedobjectstorejava.store.model.BatchCommit;
import com.github.h3yow.versionedobjectstorejava.store.model.BatchItem;
import com.github.h3yow.versionedobjectstorejava.store.model.BatchResult;
import com.github.h3yow.versionedobjectstorejava.store.model.RetentionPolicy;
import com.github.h3yow.versionedobjectstorejava.store.model.VersionInfo;
import com.github.h3yow.versionedobjectstorejava.store.model.WriteRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 批量提交测试：原子性与并发读取、批内去重与容量边界、整批拒绝
 * （权限/版本冲突/保留规则）、崩溃重启恢复、同一标识幂等重试。
 */
class BatchCommitTest {

    private static final Logger log = LoggerFactory.getLogger(BatchCommitTest.class);
    private static final String ADMIN = "alice";
    private static final String WRITER = "writer";
    private static final String READER = "reader";

    @TempDir
    Path root;

    private LocalObjectStore newStore(long quota) {
        LocalAccessControl acl = new LocalAccessControl();
        LocalObjectStore s = new LocalObjectStore(root, quota, acl);
        s.createBucket("b", ADMIN);
        acl.grant("b", WRITER, Role.WRITER);
        acl.grant("b", READER, Role.READER);
        return s;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static BatchCommit batch(String id, BatchItem... items) {
        return new BatchCommit(id, List.of(items));
    }

    // ------------------------------------------------ 原子性与并发读取

    @Test
    void batchIsAtomicAndReadersNeverSeePartialState() throws Exception {
        LocalObjectStore store = newStore(10_000_000);
        VersionInfo base = store.put("b", "doc", WriteRequest.of(bytes("doc-v1")), WRITER);
        log.info("atomicity input: existing b/doc v{} (will be overwritten), "
                + "plus new objects b/att-1, b/att-2 in one batch", base.version());

        // 提交记录落盘后、HEAD 翻转前暂停提交线程，放大“提交进行中”的窗口
        CountDownLatch recordDurable = new CountDownLatch(1);
        CountDownLatch releaseFlip = new CountDownLatch(1);
        store.setBatchCommitHookForTesting(() -> {
            recordDurable.countDown();
            try {
                releaseFlip.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        BatchCommit commit = batch("batch-atomic",
                new BatchItem("doc", new WriteRequest(bytes("doc-v2"), 1L, null)),
                BatchItem.of("att-1", bytes("attachment-1")),
                BatchItem.of("att-2", bytes("attachment-2")));
        AtomicReference<BatchResult> result = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread committer = new Thread(() -> {
            try {
                result.set(store.commitBatch("b", commit, WRITER));
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        committer.start();

        // 提交进行中（提交记录已持久化、HEAD 尚未翻转）：读者必须看到整批之前的旧状态
        assertTrue(recordDurable.await(10, TimeUnit.SECONDS));
        for (int i = 0; i < 50; i++) {
            assertArrayEquals(bytes("doc-v1"), store.get("b", "doc", READER),
                    "mid-commit read of overwritten object must see old version");
            StoreException e1 = assertThrows(StoreException.class,
                    () -> store.get("b", "att-1", READER));
            StoreException e2 = assertThrows(StoreException.class,
                    () -> store.get("b", "att-2", READER));
            assertEquals(ErrorCode.OBJECT_NOT_FOUND, e1.code());
            assertEquals(ErrorCode.OBJECT_NOT_FOUND, e2.code());
        }
        log.info("atomicity decision basis: 50 mid-commit reads all saw pre-batch state "
                + "(doc=v1, att-1/att-2 absent), batchId=batch-atomic not yet visible");

        releaseFlip.countDown();
        committer.join(10_000);
        assertNull(failure.get());
        BatchResult r = result.get();
        assertFalse(r.idempotentReplay());
        log.info("atomicity result: batchId={} committed versions={}",
                r.batchId(), r.versions().stream().map(v -> v.key() + "@v" + v.version()).toList());

        // 提交完成后整批同时可见；被覆盖对象版本号单调递增且历史可查
        assertArrayEquals(bytes("doc-v2"), store.get("b", "doc", READER));
        assertArrayEquals(bytes("attachment-1"), store.get("b", "att-1", READER));
        assertArrayEquals(bytes("attachment-2"), store.get("b", "att-2", READER));
        assertEquals(2, store.headVersion("b", "doc", READER));
        List<VersionInfo> docVersions = store.listVersions("b", "doc", READER);
        assertEquals(List.of(1L, 2L), docVersions.stream().map(VersionInfo::version).toList());
        assertArrayEquals(bytes("doc-v1"), store.get("b", "doc", 1, READER));
        assertEquals(1, store.headVersion("b", "att-1", READER));
        log.info("atomicity decision basis: after commit all 3 objects visible; "
                + "b/doc versions={} (monotonic, history intact)",
                docVersions.stream().map(VersionInfo::version).toList());
    }

    // ------------------------------------------------ 批内去重与容量边界

    @Test
    void batchDeduplicatesWithinBatchAndAgainstExistingBlobs() throws Exception {
        LocalObjectStore store = newStore(10_000_000);
        store.put("b", "existing", WriteRequest.of(bytes("shared-content")), WRITER);

        BatchResult r = store.commitBatch("b", batch("batch-dedup",
                BatchItem.of("a", bytes("shared-content")),   // 与已有内容去重
                BatchItem.of("c", bytes("dup-content")),      // 批内重复内容
                BatchItem.of("d", bytes("dup-content"))), WRITER); // 只存一份
        log.info("dedup input: batchId=batch-dedup items a=existing-content, c/d=same new content; "
                + "versions={}", r.versions().stream().map(v -> v.key() + "@v" + v.version()).toList());

        try (var s = Files.list(root.resolve("blobs"))) {
            long blobs = s.filter(p -> !p.getFileName().toString().endsWith(".tmp")).count();
            log.info("dedup decision basis: blob count={} (expect 2: shared-content + dup-content)", blobs);
            assertEquals(2, blobs);
        }
        assertEquals(r.versions().get(1).sha256(), r.versions().get(2).sha256(),
                "identical content in one batch must share one blob");
    }

    @Test
    void quotaRejectsWholeBatchAndLeavesNoResidualUsage() {
        LocalObjectStore store = newStore(100);
        store.put("b", "seed", WriteRequest.of(bytes("S".repeat(40))), WRITER); // used=40

        // 净新增 61 > 剩余 60：整批拒绝
        BatchCommit tooBig = batch("batch-quota",
                BatchItem.of("x", bytes("X".repeat(30))),
                BatchItem.of("y", bytes("Y".repeat(31))));
        StoreException e = assertThrows(StoreException.class,
                () -> store.commitBatch("b", tooBig, WRITER));
        log.info("quota input: used=40 quota=100 batch net-new=61; rejection={}", e.code());
        assertEquals(ErrorCode.QUOTA_EXCEEDED, e.code());

        // 整批不可见
        assertThrows(StoreException.class, () -> store.get("b", "x", READER));
        assertThrows(StoreException.class, () -> store.get("b", "y", READER));

        // 拒绝后无残余占用：净新增恰好 60 的批次应当成功（容量边界）
        BatchResult fits = store.commitBatch("b", batch("batch-fits",
                BatchItem.of("x", bytes("X".repeat(30))),
                BatchItem.of("y", bytes("Y".repeat(30)))), WRITER);
        log.info("quota decision basis: after rejection a net-new=60 batch committed as {}; "
                + "proves rejected batch left no residual usage",
                fits.versions().stream().map(v -> v.key() + "@v" + v.version()).toList());
        assertArrayEquals(bytes("X".repeat(30)), store.get("b", "x", READER));

        // 批内去重只计一次：相同内容 50B 写两个对象，净新增 50 而非 100
        // 用独立目录验证批内去重计费
        try {
            Path dir2 = Files.createDirectory(root.resolve("second"));
            LocalAccessControl acl2 = new LocalAccessControl();
            LocalObjectStore store2 = new LocalObjectStore(dir2, 60, acl2);
            store2.createBucket("b", ADMIN);
            acl2.grant("b", WRITER, Role.WRITER);
            BatchResult r2 = store2.commitBatch("b", batch("batch-dedup-quota",
                    BatchItem.of("p", bytes("Z".repeat(50))),
                    BatchItem.of("q", bytes("Z".repeat(50)))), WRITER);
            log.info("quota decision basis: batchId=batch-dedup-quota two items share 50B content, "
                    + "quota=60 committed as {} (would exceed if counted twice)",
                    r2.versions().stream().map(v -> v.key() + "@v" + v.version()).toList());
        } catch (java.io.IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    // ------------------------------------------------ 整批拒绝：权限 / 版本冲突 / 保留规则

    @Test
    void batchIsRejectedWhollyOnAccessDenied() {
        LocalObjectStore store = newStore(10_000_000);
        store.put("b", "keep", WriteRequest.of(bytes("keep-v1")), WRITER);

        StoreException e = assertThrows(StoreException.class, () ->
                store.commitBatch("b", batch("batch-acl",
                        BatchItem.of("keep", bytes("keep-v2")),
                        BatchItem.of("new-obj", bytes("n"))), READER)); // 只有读权限
        log.info("acl input: principal={} (READER) batch write; rejection={}", READER, e.code());
        assertEquals(ErrorCode.ACCESS_DENIED, e.code());

        // 整批未落：覆盖未生效，新对象不存在
        assertArrayEquals(bytes("keep-v1"), store.get("b", "keep", READER));
        assertThrows(StoreException.class, () -> store.get("b", "new-obj", READER));
        log.info("acl decision basis: keep=v1 unchanged, new-obj absent -> whole batch rejected");
    }

    @Test
    void batchIsRejectedWhollyOnVersionConflict() {
        LocalObjectStore store = newStore(10_000_000);
        store.put("b", "conflict", WriteRequest.of(bytes("c-v1")), WRITER);

        StoreException e = assertThrows(StoreException.class, () ->
                store.commitBatch("b", batch("batch-vc",
                        BatchItem.of("fresh", bytes("f-v1")),
                        new BatchItem("conflict", new WriteRequest(bytes("c-v2"), 5L, null))), WRITER));
        log.info("version input: item conflict expectedVersion=5 but current=1; rejection={}", e.code());
        assertEquals(ErrorCode.VERSION_CONFLICT, e.code());

        // 整批未落：同批另一个对象也不可见
        assertThrows(StoreException.class, () -> store.get("b", "fresh", READER));
        assertEquals(1, store.headVersion("b", "conflict", READER));
        assertArrayEquals(bytes("c-v1"), store.get("b", "conflict", READER));
        log.info("version decision basis: fresh absent, conflict still v1 -> whole batch rejected");
    }

    @Test
    void batchIsRejectedWhollyOnRetentionRules() {
        LocalObjectStore store = newStore(10_000_000);
        store.put("b", "locked", WriteRequest.of(bytes("locked-v1")), WRITER);
        store.put("b", "retained", WriteRequest.of(bytes("retained-v1")), WRITER);
        store.setRetention("b", "locked", new RetentionPolicy(null, true), WRITER);
        store.setRetention("b", "retained",
                RetentionPolicy.until(Instant.now().plus(Duration.ofDays(1))), WRITER);

        StoreException locked = assertThrows(StoreException.class, () ->
                store.commitBatch("b", batch("batch-lock",
                        BatchItem.of("other", bytes("o-v1")),
                        BatchItem.of("locked", bytes("locked-v2"))), WRITER));
        log.info("retention input: overwrite compliance-locked object; rejection={}", locked.code());
        assertEquals(ErrorCode.COMPLIANCE_LOCKED, locked.code());

        StoreException retained = assertThrows(StoreException.class, () ->
                store.commitBatch("b", batch("batch-retain",
                        BatchItem.of("other", bytes("o-v1")),
                        BatchItem.of("retained", bytes("retained-v2"))), WRITER));
        log.info("retention input: overwrite object within retention period; rejection={}",
                retained.code());
        assertEquals(ErrorCode.RETENTION_ACTIVE, retained.code());

        // 两次均为整批拒绝：other 不可见，目标对象保持 v1
        assertThrows(StoreException.class, () -> store.get("b", "other", READER));
        assertArrayEquals(bytes("locked-v1"), store.get("b", "locked", READER));
        assertArrayEquals(bytes("retained-v1"), store.get("b", "retained", READER));
        log.info("retention decision basis: other absent, locked/retained still v1 "
                + "-> both batches rejected wholly");
    }

    @Test
    void invalidBatchStructureIsRejected() {
        LocalObjectStore store = newStore(10_000_000);

        StoreException blankId = assertThrows(StoreException.class, () ->
                store.commitBatch("b", batch("  ", BatchItem.of("k", bytes("v"))), WRITER));
        assertEquals(ErrorCode.INVALID_BATCH, blankId.code());

        StoreException empty = assertThrows(StoreException.class, () ->
                store.commitBatch("b", new BatchCommit("batch-empty", List.of()), WRITER));
        assertEquals(ErrorCode.INVALID_BATCH, empty.code());

        StoreException dup = assertThrows(StoreException.class, () ->
                store.commitBatch("b", batch("batch-dup-key",
                        BatchItem.of("k", bytes("v1")),
                        BatchItem.of("k", bytes("v2"))), WRITER));
        log.info("structure input: blank id / empty items / duplicate key; "
                + "rejections={}, {}, {}", blankId.code(), empty.code(), dup.code());
        assertEquals(ErrorCode.INVALID_BATCH, dup.code());
        assertThrows(StoreException.class, () -> store.get("b", "k", READER));
    }

    // ------------------------------------------------ 崩溃恢复

    @Test
    void crashAfterCommitRecordIsCompletedOnRestart() {
        LocalObjectStore store = newStore(10_000_000);
        store.put("b", "doc", WriteRequest.of(bytes("doc-v1")), WRITER);

        // 崩溃点：提交记录（提交点）已持久化，HEAD 尚未翻转
        store.setBatchCommitHookForTesting(() -> {
            throw new SimulatedCrashException();
        });
        BatchCommit commit = batch("batch-crash-after",
                BatchItem.of("doc", bytes("doc-v2")),
                BatchItem.of("att", bytes("att-v1")));
        assertThrows(SimulatedCrashException.class, () -> store.commitBatch("b", commit, WRITER));
        log.info("crash-after input: batchId=batch-crash-after crashed after commit record durable");

        // 重启恢复：整批必须算提交成功并完整可读
        LocalObjectStore restarted = newStore(10_000_000);
        assertArrayEquals(bytes("doc-v2"), restarted.get("b", "doc", READER));
        assertArrayEquals(bytes("att-v1"), restarted.get("b", "att", READER));
        assertEquals(2, restarted.headVersion("b", "doc", READER));
        log.info("crash-after decision basis: recovery replayed commit record; "
                + "doc=v2 att=v1 both visible");

        // 重启后用同一标识重试：幂等回放，不产生新版本
        BatchResult replay = restarted.commitBatch("b", commit, WRITER);
        assertTrue(replay.idempotentReplay());
        assertEquals(List.of(2L, 1L),
                replay.versions().stream().map(VersionInfo::version).toList());
        assertEquals(2, restarted.headVersion("b", "doc", READER));
        assertEquals(1, restarted.headVersion("b", "att", READER));
        log.info("crash-after decision basis: retry batchId=batch-crash-after replayed versions={}",
                replay.versions().stream().map(v -> v.key() + "@v" + v.version()).toList());
    }

    @Test
    void crashBeforeCommitRecordIsDiscardedOnRestart() {
        LocalObjectStore store = newStore(10_000_000);
        store.put("b", "doc", WriteRequest.of(bytes("doc-v1")), WRITER);

        // 崩溃点：版本元数据已落盘，提交记录尚未写出（整批未达提交点）
        store.setBatchPrepareHookForTesting(() -> {
            throw new SimulatedCrashException();
        });
        BatchCommit commit = batch("batch-crash-before",
                BatchItem.of("doc", bytes("doc-v2")),
                BatchItem.of("att", bytes("att-v1")));
        assertThrows(SimulatedCrashException.class, () -> store.commitBatch("b", commit, WRITER));
        log.info("crash-before input: batchId=batch-crash-before crashed before commit record");

        // 重启恢复：整批完全不算数
        LocalObjectStore restarted = newStore(10_000_000);
        assertArrayEquals(bytes("doc-v1"), restarted.get("b", "doc", READER));
        assertEquals(1, restarted.headVersion("b", "doc", READER));
        assertThrows(StoreException.class, () -> restarted.get("b", "att", READER));
        assertFalse(Files.exists(root.resolve("buckets/b/objects/doc/versions/v-2.json")),
                "dangling batch metadata must be cleaned by recovery");
        log.info("crash-before decision basis: doc=v1, att absent, dangling v-2.json removed");

        // 同一标识重试：重新执行成功，版本号确定一致（HEAD 从未前进）
        BatchResult r = restarted.commitBatch("b", commit, WRITER);
        assertFalse(r.idempotentReplay());
        assertEquals(List.of(2L, 1L), r.versions().stream().map(VersionInfo::version).toList());
        assertArrayEquals(bytes("doc-v2"), restarted.get("b", "doc", READER));
        assertArrayEquals(bytes("att-v1"), restarted.get("b", "att", READER));
        log.info("crash-before decision basis: retry batchId=batch-crash-before committed as {}",
                r.versions().stream().map(v -> v.key() + "@v" + v.version()).toList());
    }

    // ------------------------------------------------ 幂等重试

    @Test
    void retryWithSameBatchIdIsIdempotent() throws Exception {
        LocalObjectStore store = newStore(10_000_000);
        BatchCommit commit = batch("batch-idem",
                BatchItem.of("m", bytes("m-v1")),
                BatchItem.of("n", bytes("n-v1")));

        BatchResult first = store.commitBatch("b", commit, WRITER);
        assertFalse(first.idempotentReplay());
        long blobsAfterFirst;
        try (var s = Files.list(root.resolve("blobs"))) {
            blobsAfterFirst = s.filter(p -> !p.getFileName().toString().endsWith(".tmp")).count();
        }

        // 网络抖动重试：同一标识同一内容
        BatchResult retry = store.commitBatch("b", commit, WRITER);
        log.info("idempotency input: retry batchId=batch-idem; first={} retry={}",
                first.versions().stream().map(v -> v.key() + "@v" + v.version()).toList(),
                retry.versions().stream().map(v -> v.key() + "@v" + v.version()).toList());
        assertTrue(retry.idempotentReplay());
        assertEquals(first.versions(), retry.versions(), "retry must return the stable result");

        // 不重复产生版本、不重复占用空间
        assertEquals(1, store.headVersion("b", "m", READER));
        assertEquals(1, store.headVersion("b", "n", READER));
        try (var s = Files.list(root.resolve("blobs"))) {
            long blobs = s.filter(p -> !p.getFileName().toString().endsWith(".tmp")).count();
            log.info("idempotency decision basis: blob count {} == {} (no extra space), heads stay 1",
                    blobs, blobsAfterFirst);
            assertEquals(blobsAfterFirst, blobs);
        }

        // 同一标识被不同内容复用：拒绝且原因可区分
        StoreException reused = assertThrows(StoreException.class, () ->
                store.commitBatch("b", batch("batch-idem",
                        BatchItem.of("m", bytes("DIFFERENT"))), WRITER));
        log.info("idempotency input: batchId=batch-idem reused with different content; rejection={}",
                reused.code());
        assertEquals(ErrorCode.BATCH_ID_REUSED, reused.code());
    }

    /** 模拟进程崩溃的标记异常。 */
    private static final class SimulatedCrashException extends RuntimeException {
    }
}
