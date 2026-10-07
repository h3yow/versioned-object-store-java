package com.github.h3yow.versionedobjectstorejava.store;

import com.github.h3yow.versionedobjectstorejava.store.acl.LocalAccessControl;
import com.github.h3yow.versionedobjectstorejava.store.acl.Role;
import com.github.h3yow.versionedobjectstorejava.store.local.LocalObjectStore;
import com.github.h3yow.versionedobjectstorejava.store.model.BatchItem;
import com.github.h3yow.versionedobjectstorejava.store.model.BatchResult;
import com.github.h3yow.versionedobjectstorejava.store.model.VersionInfo;
import com.github.h3yow.versionedobjectstorejava.store.model.WriteRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 批量提交测试：原子可见性与并发读取、批内去重与容量边界、
 * 权限/版本条件/保留规则导致的整批拒绝、中断重启恢复、同标识幂等重试。
 */
class BatchCommitTest {

    private static final Logger log = LoggerFactory.getLogger(BatchCommitTest.class);
    private static final String ADMIN = "alice";

    @TempDir
    Path root;

    LocalAccessControl acl;
    LocalObjectStore store;

    @BeforeEach
    void setUp() {
        acl = new LocalAccessControl();
        store = new LocalObjectStore(root, 10_000_000, acl);
        store.createBucket("docs", ADMIN);
        acl.grant("docs", "bob", Role.WRITER);
        acl.grant("docs", "carol", Role.READER);
    }

    private static BatchItem item(String key, String content) {
        return new BatchItem(key, WriteRequest.of(content.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void batchCommitsAllObjectsAtomicallyAndMonotonically() {
        // 预置：a 已有 v1，b 是全新对象，c 已有 v1
        store.put("docs", "a", WriteRequest.of("a-old".getBytes()), "bob");
        store.put("docs", "c", WriteRequest.of("c-old".getBytes()), "bob");

        List<BatchItem> items = List.of(
                item("a", "a-new"),
                item("b", "b-new"),
                item("c", "c-new"));
        BatchResult result = store.putBatch("batch-001", "docs", items, "bob");
        log.info("batch input: batchId={}, items={}", "batch-001",
                items.stream().map(BatchItem::key).toList());
        log.info("batch committed versions: {}",
                result.versions().stream().map(v -> v.key() + "=v" + v.version()).toList());

        assertEquals(List.of(2L, 1L, 2L),
                result.versions().stream().map(VersionInfo::version).toList(),
                "each object follows its own monotonic version sequence");
        assertTrue(result.versions().stream().allMatch(v -> "batch-001".equals(v.batchId())));

        assertArrayEquals("a-new".getBytes(), store.get("docs", "a", "bob"));
        assertArrayEquals("b-new".getBytes(), store.get("docs", "b", "carol"));
        assertArrayEquals("c-new".getBytes(), store.get("docs", "c", "carol"));

        // 历史版本不被破坏，版本号顺序照旧可查
        assertArrayEquals("a-old".getBytes(), store.get("docs", "a", 1, "carol"));
        List<Long> aHistory = store.listVersions("docs", "a", "carol").stream()
                .map(VersionInfo::version).toList();
        log.info("batch decision basis: history of a = {} (expect [1, 2])", aHistory);
        assertEquals(List.of(1L, 2L), aHistory);
        assertEquals(2, store.headVersion("docs", "a", "carol"));
        assertEquals(1, store.headVersion("docs", "b", "carol"));
    }

    @Test
    void readersSeeNothingOfBatchUntilCommitMarkerPublished() throws Exception {
        store.putBatch("init", "docs",
                List.of(item("k1", "gen0"), item("k2", "gen0"), item("k3", "gen0")), "bob");

        // 在“所有 HEAD 已推进、committed 标记尚未发布”的窗口卡住提交
        CountDownLatch reachedWindow = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        store.setBatchPreCommitHookForTesting(() -> {
            reachedWindow.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<?> commit = pool.submit(() ->
                store.putBatch("gen1", "docs",
                        List.of(item("k1", "gen1"), item("k2", "gen1"), item("k3", "gen1")), "bob"));
        assertTrue(reachedWindow.await(10, TimeUnit.SECONDS), "commit must reach pre-marker window");

        // 窗口内：HEAD 文件已指向新版本，但标记不存在——读者必须一律看到旧的 gen0
        for (String k : List.of("k1", "k2", "k3")) {
            Path headFile = root.resolve("buckets/docs/objects/" + k + "/HEAD");
            long headOnDisk = Long.parseLong(Files.readString(headFile).trim());
            String visible = new String(store.get("docs", k, "carol"), StandardCharsets.UTF_8);
            log.info("mid-batch window: {} HEAD-file={} visible={}", k, headOnDisk, visible);
            assertEquals(2, headOnDisk, "HEAD file must already point at v2 during the window for " + k);
            assertEquals(1, store.headVersion("docs", k, "carol"),
                    "visible head must stay v1 during the window for " + k);
            assertEquals("gen0", visible, "batch version must stay invisible before marker: " + k);
            List<Long> visibleVersions = store.listVersions("docs", k, "carol").stream()
                    .map(VersionInfo::version).toList();
            assertEquals(List.of(1L), visibleVersions, "uncommitted v2 must not be listed for " + k);
        }

        release.countDown();
        commit.get(10, TimeUnit.SECONDS);
        pool.shutdown();

        for (String k : List.of("k1", "k2", "k3")) {
            assertEquals("gen1", new String(store.get("docs", k, "carol"), StandardCharsets.UTF_8));
        }
        log.info("batch-window decision basis: invisible during window, all gen1 after release");
    }

    @Test
    void concurrentReadersNeverSeePartialBatch() throws Exception {
        // 预放三个对象的初始版本：读者始终应看到三个对象同代
        store.putBatch("init", "docs",
                List.of(item("k1", "gen0"), item("k2", "gen0"), item("k3", "gen0")), "bob");

        int readers = 12;
        ExecutorService pool = Executors.newFixedThreadPool(readers + 1);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> violations = new ConcurrentLinkedQueue<>();
        List<Future<?>> futures = new ArrayList<>();

        // 提交多个批次，每批按 k1,k2,k3 顺序同时覆盖三个对象
        futures.add(pool.submit(() -> {
            start.await();
            for (int g = 1; g <= 30; g++) {
                String gen = "gen" + g;
                store.putBatch("batch-gen" + g, "docs",
                        List.of(item("k1", gen), item("k2", gen), item("k3", gen)), "bob");
            }
            return null;
        }));
        for (int t = 0; t < readers; t++) {
            futures.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < 300; i++) {
                    // 三次读沿时间轴进行；整批原子可见时，按提交顺序观测到的代际
                    // 必须单调不减。若某键提前可见（逐键提交），会观测到代际回落。
                    int g1 = genOf(store.get("docs", "k1", "carol"));
                    int g2 = genOf(store.get("docs", "k2", "carol"));
                    int g3 = genOf(store.get("docs", "k3", "carol"));
                    if (!(g1 <= g2 && g2 <= g3)) {
                        violations.add("non-atomic generations observed: "
                                + g1 + "," + g2 + "," + g3);
                    }
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(60, TimeUnit.SECONDS);
        }
        pool.shutdown();
        log.info("batch-rw input: 30 batches x 3 keys, {} readers; violations={}", readers, violations);
        assertTrue(violations.isEmpty(),
                "readers must never observe a key flipping earlier than the rest of the batch: "
                        + violations.peek());
    }

    private static int genOf(byte[] content) {
        return Integer.parseInt(new String(content, StandardCharsets.UTF_8).substring(3));
    }

    @Test
    void batchDeduplicatesWithinBatchAndAgainstExistingContent() throws Exception {
        byte[] shared = "shared-content".getBytes(StandardCharsets.UTF_8);
        // 已有对象占用一份 shared 内容
        store.put("docs", "existing", WriteRequest.of(shared), "bob");

        List<BatchItem> items = List.of(
                new BatchItem("a", WriteRequest.of(shared)),
                new BatchItem("b", WriteRequest.of(shared)),
                new BatchItem("c", WriteRequest.of(shared)));
        BatchResult result = store.putBatch("dedup-batch", "docs", items, "bob");
        log.info("dedup-batch input: batchId=dedup-batch, 3 items x {} bytes, content already stored once",
                shared.length);
        log.info("dedup-batch versions: {}",
                result.versions().stream().map(v -> v.key() + "=v" + v.version()).toList());

        try (var s = Files.list(root.resolve("blobs"))) {
            long blobs = s.filter(p -> !p.getFileName().toString().endsWith(".tmp")).count();
            log.info("dedup-batch decision basis: blob count = {} (expect 1)", blobs);
            assertEquals(1, blobs, "identical content in batch and existing data must share one blob");
        }
        // 净新增为 0：再验证三个对象都能读到
        for (String k : List.of("existing", "a", "b", "c")) {
            assertArrayEquals(shared, store.get("docs", k, "carol"));
        }
    }

    @Test
    void batchQuotaIsCountedOnNetNewBytesAndRejectionLeavesNoTrace() throws Exception {
        LocalObjectStore small = new LocalObjectStore(root.resolve("small"), 100, acl);
        small.createBucket("b", ADMIN);

        byte[] existing = new byte[60]; // 全零，已占 60
        small.put("b", "existing", WriteRequest.of(existing), ADMIN);

        byte[] fresh = new byte[50];
        java.util.Arrays.fill(fresh, (byte) 7); // 净新增 50：60+50>100，整批拒绝
        StoreException ex = assertThrows(StoreException.class, () ->
                small.putBatch("too-big", "b",
                        List.of(new BatchItem("a", WriteRequest.of(existing)), // 命中去重，0 新增
                                new BatchItem("c", WriteRequest.of(fresh))),   // 50 新增
                        ADMIN));
        log.info("quota-batch input: quota=100 used=60 netNew=50; rejection={}", ex.code());
        assertEquals(ErrorCode.QUOTA_EXCEEDED, ex.code());

        // 拒绝后不得残留：对象不存在、无新版本文件、无新内容块、无批次目录、空间占用不变
        for (String k : List.of("a", "c")) {
            StoreException nf = assertThrows(StoreException.class, () -> small.get("b", k, ADMIN));
            assertEquals(ErrorCode.OBJECT_NOT_FOUND, nf.code());
        }
        try (var blobs = Files.list(root.resolve("small/blobs"))) {
            long blobCount = blobs.filter(p -> !p.getFileName().toString().endsWith(".tmp")).count();
            assertEquals(1, blobCount, "rejected batch must not leave new blobs behind");
        }
        assertFalse(Files.exists(root.resolve("small/batches/too-big")),
                "rejected batch must not leave a journal directory");
        log.info("quota-batch decision basis: no new objects/blobs/journal after rejection");

        // 边界：净新增恰好不超配额（去重命中 0 + 新内容 40 = 100）应成功
        byte[] fitting = new byte[40];
        java.util.Arrays.fill(fitting, (byte) 9);
        BatchResult ok = small.putBatch("fits", "b",
                List.of(new BatchItem("a", WriteRequest.of(existing)),
                        new BatchItem("d", WriteRequest.of(fitting))),
                ADMIN);
        log.info("quota-batch boundary input: netNew=40, committed versions: {}",
                ok.versions().stream().map(v -> v.key() + "=v" + v.version()).toList());
        assertEquals(1, ok.versions().get(0).version());
        assertEquals(1, ok.versions().get(1).version());
    }

    @Test
    void batchIsRejectedWholeWhenAnyItemLacksPermissionOrHasVersionConflict() {
        store.put("docs", "a", WriteRequest.of("a1".getBytes()), "bob");
        store.put("docs", "c", WriteRequest.of("c1".getBytes()), "bob");

        // 1) 任一对象（对提交者）无写权限：carol 只有 READER
        StoreException denied = assertThrows(StoreException.class, () ->
                store.putBatch("no-perm", "docs",
                        List.of(item("a", "a2"), item("b", "b1")), "carol"));
        log.info("reject-acl input: principal=carol(READER); rejection={}", denied.code());
        assertEquals(ErrorCode.ACCESS_DENIED, denied.code());
        // 整批不落：b 不存在，a 仍是 v1
        assertEquals(1, store.headVersion("docs", "a", "carol"));
        assertThrows(StoreException.class, () -> store.get("docs", "b", "carol"));

        // 2) 批内某项版本条件冲突（a 当前 v1，却声明期望 v5）；其他项合法也要整批拒绝
        StoreException conflict = assertThrows(StoreException.class, () ->
                store.putBatch("stale-cond", "docs",
                        List.of(new BatchItem("a", new WriteRequest("a2".getBytes(), 5L, null)),
                                item("b", "b1")), "bob"));
        log.info("reject-cas input: a expectedVersion=5 actual=1; rejection={}", conflict.code());
        assertEquals(ErrorCode.VERSION_CONFLICT, conflict.code());
        assertEquals(1, store.headVersion("docs", "a", "carol"));
        assertThrows(StoreException.class, () -> store.get("docs", "b", "carol"));
        assertFalse(java.nio.file.Files.exists(root.resolve("batches/stale-cond")),
                "version-conflict batch must leave no journal");
    }

    @Test
    void batchIsRejectedWholeWhenRetentionOrComplianceLocksTheObject() {
        // 保留期内的对象禁止覆盖
        store.setRetention("docs", "ret",
                com.github.h3yow.versionedobjectstorejava.store.model.RetentionPolicy
                        .until(java.time.Instant.now().plus(java.time.Duration.ofHours(1))), "bob");
        store.put("docs", "ret", WriteRequest.of("r1".getBytes()), "bob");
        StoreException retention = assertThrows(StoreException.class, () ->
                store.putBatch("ret-batch", "docs",
                        List.of(item("ret", "r2"), item("x", "x1")), "bob"));
        log.info("reject-retention input: ret retained (future); rejection={}", retention.code());
        assertEquals(ErrorCode.RETENTION_ACTIVE, retention.code());
        assertArrayEquals("r1".getBytes(), store.get("docs", "ret", "carol"));
        assertThrows(StoreException.class, () -> store.get("docs", "x", "carol"));

        // 合规锁定的对象禁止覆盖
        store.setRetention("docs", "lock",
                new com.github.h3yow.versionedobjectstorejava.store.model.RetentionPolicy(null, true), "bob");
        store.put("docs", "lock", WriteRequest.of("l1".getBytes()), "bob");
        StoreException locked = assertThrows(StoreException.class, () ->
                store.putBatch("lock-batch", "docs",
                        List.of(item("y", "y1"), item("lock", "l2")), "bob"));
        log.info("reject-compliance input: lock compliance-locked; rejection={}", locked.code());
        assertEquals(ErrorCode.COMPLIANCE_LOCKED, locked.code());
        assertArrayEquals("l1".getBytes(), store.get("docs", "lock", "carol"));
        assertThrows(StoreException.class, () -> store.get("docs", "y", "carol"));

        // 拒绝原因彼此可区分，且没有批次痕迹
        assertFalse(Files.exists(root.resolve("batches/ret-batch")));
        assertFalse(Files.exists(root.resolve("batches/lock-batch")));
    }

    @Test
    void malformedBatchesAreRejected() {
        StoreException empty = assertThrows(StoreException.class, () ->
                store.putBatch("empty", "docs", List.of(), "bob"));
        log.info("reject-empty input: 0 items; rejection={}", empty.code());
        assertEquals(ErrorCode.BATCH_INVALID, empty.code());

        StoreException dup = assertThrows(StoreException.class, () ->
                store.putBatch("dup", "docs",
                        List.of(item("same", "1"), item("same", "2")), "bob"));
        log.info("reject-dup input: duplicated key 'same'; rejection={}", dup.code());
        assertEquals(ErrorCode.BATCH_INVALID, dup.code());
    }

    @Test
    void sameBatchIdRetryIsIdempotentAndReturnsStableResult() {
        store.put("docs", "a", WriteRequest.of("a-old".getBytes()), "bob");
        List<BatchItem> items = List.of(item("a", "a-new"), item("b", "b-new"), item("c", "c-new"));

        BatchResult first = store.putBatch("idem-1", "docs", items, "bob");
        BatchResult retry = store.putBatch("idem-1", "docs", items, "bob");
        BatchResult retry2 = store.putBatch("idem-1", "docs", items, "bob");
        log.info("idempotent input: batchId=idem-1 submitted 3 times; first={}, retry={}, retry2={}",
                versionList(first), versionList(retry), versionList(retry2));

        assertEquals(versionList(first), versionList(retry));
        assertEquals(versionList(first), versionList(retry2));
        assertEquals(List.of(2L, 1L, 1L),
                retry.versions().stream().map(VersionInfo::version).toList());

        // 不重复生成版本：版本历史里每个对象只有预期的版本数
        assertEquals(List.of(1L, 2L),
                store.listVersions("docs", "a", "carol").stream().map(VersionInfo::version).toList());
        assertEquals(List.of(1L),
                store.listVersions("docs", "b", "carol").stream().map(VersionInfo::version).toList());
        log.info("idempotent decision basis: histories unchanged after retries; versions still a=[1,2],b=[1]");
    }

    @Test
    void sameBatchIdWithDifferentPayloadIsRejectedDistinctly() {
        store.putBatch("x", "docs", List.of(item("a", "original")), "bob");

        // 同标识但内容不同
        StoreException diffContent = assertThrows(StoreException.class, () ->
                store.putBatch("x", "docs", List.of(item("a", "tampered")), "bob"));
        log.info("idempotent-conflict input: batchId=x retried with different content; rejection={}",
                diffContent.code());
        assertEquals(ErrorCode.BATCH_RETRY_CONFLICT, diffContent.code());

        // 同标识但键集合不同（项数不同）
        StoreException diffItems = assertThrows(StoreException.class, () ->
                store.putBatch("x", "docs", List.of(item("a", "original"), item("z", "z")), "bob"));
        log.info("idempotent-conflict input: batchId=x retried with extra item; rejection={}",
                diffItems.code());
        assertEquals(ErrorCode.BATCH_RETRY_CONFLICT, diffItems.code());

        // 原提交结果不受影响
        assertArrayEquals("original".getBytes(), store.get("docs", "a", "carol"));
        assertThrows(StoreException.class, () -> store.get("docs", "z", "carol"));
    }

    @Test
    void concurrentSameBatchIdSubmissionsCommitExactlyOnce() throws Exception {
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<BatchResult> results = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<StoreException> errors = new ConcurrentLinkedQueue<>();
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            futures.add(pool.submit(() -> {
                start.await();
                try {
                    results.add(store.putBatch("concurrent-same", "docs",
                            List.of(item("p", "p1"), item("q", "q1")), "bob"));
                } catch (StoreException e) {
                    errors.add(e);
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();

        log.info("concurrent-idempotent input: {} threads same batchId; results={}, errors={}",
                threads, results.size(), errors.stream().map(StoreException::code).toList());
        assertEquals(threads, results.size(), "identical concurrent retries all get the stable result");
        for (BatchResult r : results) {
            assertEquals(List.of(1L, 1L),
                    r.versions().stream().map(VersionInfo::version).toList());
        }
        assertEquals(1, store.headVersion("docs", "p", "carol"));
        assertEquals(1, store.headVersion("docs", "q", "carol"));
    }

    private static List<String> versionList(BatchResult r) {
        return r.versions().stream().map(v -> v.key() + "=v" + v.version()).toList();
    }
}
