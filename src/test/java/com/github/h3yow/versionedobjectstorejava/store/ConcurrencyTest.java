
package com.github.h3yow.versionedobjectstorejava.store;

import com.github.h3yow.versionedobjectstorejava.store.acl.LocalAccessControl;
import com.github.h3yow.versionedobjectstorejava.store.acl.Role;
import com.github.h3yow.versionedobjectstorejava.store.local.LocalObjectStore;
import com.github.h3yow.versionedobjectstorejava.store.model.VersionInfo;
import com.github.h3yow.versionedobjectstorejava.store.model.WriteRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 并发提交与并发读写一致性测试。
 */
class ConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyTest.class);
    private static final String ADMIN = "alice";
    private static final int THREADS = 16;

    @TempDir
    Path root;

    LocalObjectStore store;

    @BeforeEach
    void setUp() {
        LocalAccessControl acl = new LocalAccessControl();
        store = new LocalObjectStore(root, 100_000_000, acl);
        store.createBucket("c", ADMIN);
        acl.grant("c", "writer", Role.WRITER);
        acl.grant("c", "reader", Role.READER);
    }

    @Test
    void concurrentCommitsProduceMonotonicGaplessVersions() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<VersionInfo> committed = new ConcurrentLinkedQueue<>();
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) {
            int n = i;
            futures.add(pool.submit(() -> {
                start.await();
                // 默认语义：无条件追加；并发下版本号必须唯一且单调无间隙
                VersionInfo info = store.put("c", "hot",
                        WriteRequest.of(("payload-" + n).getBytes(StandardCharsets.UTF_8)), "writer");
                committed.add(info);
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();

        Set<Long> versions = new HashSet<>();
        for (VersionInfo info : committed) {
            versions.add(info.version());
        }
        log.info("concurrency input: {} threads committing same object; committed versions = {}",
                THREADS, versions.stream().sorted().toList());
        assertEquals(THREADS, versions.size(), "every commit must get a distinct version");

        long head = store.headVersion("c", "hot", "reader");
        List<VersionInfo> listed = store.listVersions("c", "hot", "reader");
        log.info("concurrency decision basis: head={}, listed={}",
                head, listed.stream().map(VersionInfo::version).toList());
        assertEquals(THREADS, head, "head must equal number of successful commits");
        assertEquals(THREADS, listed.size(), "no half-committed or lost versions");
        for (long v = 1; v <= THREADS; v++) {
            assertTrue(versions.contains(v), "version " + v + " must exist (gapless)");
        }
    }

    @Test
    void concurrentOptimisticCommitsExactlyOneWinsPerRound() throws Exception {
        store.put("c", "cas", WriteRequest.of("v1".getBytes()), "writer");
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<VersionInfo> wins = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<StoreException> conflicts = new ConcurrentLinkedQueue<>();
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) {
            int n = i;
            futures.add(pool.submit(() -> {
                start.await();
                try {
                    wins.add(store.put("c", "cas",
                            new WriteRequest(("w" + n).getBytes(), 1L, null), "writer"));
                } catch (StoreException e) {
                    conflicts.add(e);
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();

        log.info("cas input: {} threads with expectedVersion=1; wins={}, conflicts={}",
                THREADS, wins.size(), conflicts.size());
        assertEquals(1, wins.size(), "exactly one optimistic commit may succeed");
        assertEquals(2L, wins.peek().version());
        assertTrue(conflicts.stream().allMatch(e -> e.code() == ErrorCode.VERSION_CONFLICT),
                "all losers must see VERSION_CONFLICT");
        assertEquals(THREADS - 1, conflicts.size());
    }

    @Test
    void readersAlwaysSeeCompleteConsistentObjects() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> violations = new ConcurrentLinkedQueue<>();
        int rounds = 50;
        List<Future<?>> futures = new ArrayList<>();
        // 写线程：内容格式为 "gen-<i>"，读取者若看到不匹配的内容即记录违规
        futures.add(pool.submit(() -> {
            start.await();
            for (int i = 0; i < rounds; i++) {
                store.put("c", "rw", WriteRequest.of(("gen-" + i).getBytes()), "writer");
            }
            return null;
        }));
        for (int t = 0; t < THREADS - 1; t++) {
            futures.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < rounds; i++) {
                    try {
                        long head = store.headVersion("c", "rw", "reader");
                        byte[] content = store.get("c", "rw", head, "reader");
                        String s = new String(content, StandardCharsets.UTF_8);
                        if (!s.startsWith("gen-")) {
                            violations.add("corrupt content at v" + head + ": " + s);
                        }
                    } catch (StoreException e) {
                        if (e.code() != ErrorCode.OBJECT_NOT_FOUND) {
                            violations.add("unexpected rejection: " + e.code());
                        }
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
        log.info("rw-consistency input: 1 writer x {} rounds, {} readers; violations={}",
                rounds, THREADS - 1, violations);
        assertTrue(violations.isEmpty(), "readers must never observe torn or corrupt state");
    }
}
