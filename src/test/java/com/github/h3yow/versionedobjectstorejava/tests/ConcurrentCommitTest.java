package com.github.h3yow.versionedobjectstorejava.tests;

import com.github.h3yow.versionedobjectstorejava.store.BlobStore;
import com.github.h3yow.versionedobjectstorejava.store.ObjectVersion;
import com.github.h3yow.versionedobjectstorejava.store.PutRequest;
import com.github.h3yow.versionedobjectstorejava.store.PutResult;
import com.github.h3yow.versionedobjectstorejava.store.RejectReason;
import com.github.h3yow.versionedobjectstorejava.store.StoreException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.h3yow.versionedobjectstorejava.tests.StoreTestSupport.OWNER;
import static com.github.h3yow.versionedobjectstorejava.tests.StoreTestSupport.defaultSpace;
import static com.github.h3yow.versionedobjectstorejava.tests.StoreTestSupport.newEnv;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发提交/读取语义：
 * 1) N 个写线程对同一对象无条件并发提交 -> 版本号 1..N 不重不漏、单调递增；
 * 2) 提交过程中的并发读取永远看到完整一致的对象（无半提交可见）；
 * 3) 相同内容并发提交 -> 恰好一个新建块，其余全部去重；
 * 4) 相同乐观锁条件并发 -> 恰好一个成功，其余得到可区分的 VERSION_CONFLICT。
 */
class ConcurrentCommitTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentCommitTest.class);
    private static final String SPACE = "cspace";

    @Test
    void concurrentPutsProduceDenseMonotonicVersionsAndReadsAreWhole(@TempDir Path tmp)
            throws Exception {
        var env = newEnv(tmp);
        defaultSpace(env, SPACE, 100L * 1024 * 1024);

        int writers = 12;
        int readers = 6;
        int writesPerWriter = 10;

        // 先建立 v1，读者窗口内对象始终存在
        byte[] seed = "seed".getBytes(StandardCharsets.UTF_8);
        env.store().put(new PutRequest(SPACE, "hot", OWNER, seed,
                -1, BlobStore.sha256(seed), 0, false));

        ExecutorService pool = Executors.newFixedThreadPool(writers + readers);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        Set<Long> seenVersions = Collections.synchronizedSet(new HashSet<>());
        AtomicInteger integrityChecks = new AtomicInteger();

        for (int w = 0; w < writers; w++) {
            final int writerId = w;
            futures.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < writesPerWriter; i++) {
                    byte[] content = ("writer-" + writerId + "-seq-" + i).getBytes(StandardCharsets.UTF_8);
                    PutResult r = env.store().put(new PutRequest(SPACE, "hot", OWNER, content,
                            -1, BlobStore.sha256(content), 0, false));
                    seenVersions.add(r.version());
                }
                return null;
            }));
        }
        for (int r = 0; r < readers; r++) {
            futures.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < 200; i++) {
                    var vc = env.store().readLatest(SPACE, "hot", OWNER);
                    ObjectVersion meta = vc.version();
                    byte[] data = vc.content();
                    // 完整性判定依据：读到的字节哈希必须与元数据声明的 sha256 完全一致
                    assertEquals(meta.sha256(), BlobStore.sha256(data),
                            "并发读取看到的对象必须完整一致");
                    assertEquals(meta.sizeBytes(), data.length);
                    integrityChecks.incrementAndGet();
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        pool.shutdownNow();

        int expectedTotal = 1 + writers * writesPerWriter;
        log.info("关键结果: 写入总数={}, 观察到的版本号集合大小={}, 并发完整性校验次数={}",
                expectedTotal, seenVersions.size(), integrityChecks.get());
        assertEquals(writers * writesPerWriter, seenVersions.size(), "版本号不得重复");
        for (long v = 2; v <= expectedTotal; v++) {
            assertTrue(seenVersions.contains(v), "版本号必须连续不缺失: " + v);
        }
        ObjectVersion head = env.store().stat(SPACE, "hot", OWNER);
        assertEquals(expectedTotal, head.version(), "最终最新版本号必须为最大值");
        assertArrayEquals(env.store().getVersion(SPACE, "hot", expectedTotal, OWNER),
                env.store().get(SPACE, "hot", OWNER));
        log.info("判定依据: 版本集合为 1..{} 连续区间, 最终 head=v{}, 所有 {} 次并发读取均完整",
                expectedTotal, head.version(), integrityChecks.get());
    }

    @Test
    void concurrentIdenticalPutsDeduplicateToSingleBlob(@TempDir Path tmp) throws Exception {
        var env = newEnv(tmp);
        defaultSpace(env, SPACE, 10_000);

        int n = 16;
        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<PutResult>> futures = new ArrayList<>();
        byte[] content = "identical-content".getBytes(StandardCharsets.UTF_8);
        String sha = BlobStore.sha256(content);
        for (int i = 0; i < n; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                return env.store().put(new PutRequest(SPACE, "same", OWNER, content,
                        -1, sha, 0, false));
            }));
        }
        start.countDown();
        int deduplicated = 0;
        for (Future<PutResult> f : futures) {
            if (f.get(10, TimeUnit.SECONDS).deduplicated()) {
                deduplicated++;
            }
        }
        pool.shutdownNow();

        log.info("关键结果: 并发数={}, sha256={}, 去重命中数={}, 空间引用计数={}",
                n, sha, deduplicated, env.store().debugBlobRefs(SPACE));
        assertEquals(n - 1, deduplicated, "恰好一次新建块，其余全部去重");
        assertEquals(n, env.store().debugBlobRefs(SPACE).get(sha),
                "所有版本仍共享唯一内容块，引用计数=并发数");
        assertTrue(env.blobs().exists(sha));
    }

    @Test
    void concurrentOptimisticLockOnSameBaselineOnlyOneSucceeds(@TempDir Path tmp) throws Exception {
        var env = newEnv(tmp);
        defaultSpace(env, SPACE, 10_000);
        byte[] base = "base".getBytes(StandardCharsets.UTF_8);
        env.store().put(new PutRequest(SPACE, "obj", OWNER, base, -1, BlobStore.sha256(base), 0, false));

        int n = 10;
        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        AtomicInteger conflicts = new AtomicInteger();
        AtomicInteger success = new AtomicInteger();
        for (int i = 0; i < n; i++) {
            final int idx = i;
            futures.add(pool.submit(() -> {
                start.await();
                byte[] c = ("update-" + idx).getBytes(StandardCharsets.UTF_8);
                try {
                    // 所有线程都以基线 v1 提交
                    env.store().put(new PutRequest(SPACE, "obj", OWNER, c, 1L,
                            BlobStore.sha256(c), 0, false));
                    success.incrementAndGet();
                } catch (StoreException e) {
                    assertEquals(RejectReason.VERSION_CONFLICT, e.getReason());
                    conflicts.incrementAndGet();
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        pool.shutdownNow();

        log.info("关键结果: 并发乐观锁提交数={}, 成功={}, 版本冲突={}", n, success.get(), conflicts.get());
        assertEquals(1, success.get(), "同一基线下只能有一个提交成功");
        assertEquals(n - 1, conflicts.get(), "其余全部得到 VERSION_CONFLICT");
        assertEquals(2L, env.store().stat(SPACE, "obj", OWNER).version(),
                "成功提交后 head 必须为 v2，不存在半提交版本");
    }
}
