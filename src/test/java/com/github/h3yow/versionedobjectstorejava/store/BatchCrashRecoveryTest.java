package com.github.h3yow.versionedobjectstorejava.store;

import com.github.h3yow.versionedobjectstorejava.store.acl.LocalAccessControl;
import com.github.h3yow.versionedobjectstorejava.store.acl.Role;
import com.github.h3yow.versionedobjectstorejava.store.local.LocalObjectStore;
import com.github.h3yow.versionedobjectstorejava.store.model.BatchItem;
import com.github.h3yow.versionedobjectstorejava.store.model.BatchResult;
import com.github.h3yow.versionedobjectstorejava.store.model.WriteRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 批量提交中途进程被强杀后的重启恢复测试。
 *
 * <p>关键崩溃点通过测试钩子注入异常模拟：
 * 净新增内容块已落盘但日志未写入（整批不算数）、所有 HEAD 已推进但提交标记
 * 未发布（已 PREPARED，重启前滚为整批成功）、提交标记刚发布（重启后完整可见）。
 * 随后在同一目录上构造新实例模拟进程重启。</p>
 */
class BatchCrashRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(BatchCrashRecoveryTest.class);
    private static final String ADMIN = "alice";

    @TempDir
    Path root;

    private LocalObjectStore newStore() {
        LocalAccessControl acl = new LocalAccessControl();
        LocalObjectStore s = new LocalObjectStore(root, 10_000_000, acl);
        s.createBucket("docs", ADMIN);
        acl.grant("docs", "bob", Role.WRITER);
        acl.grant("docs", "carol", Role.READER);
        return s;
    }

    private static BatchItem item(String key, String content) {
        return new BatchItem(key, WriteRequest.of(content.getBytes(StandardCharsets.UTF_8)));
    }

    private static final class SimulatedCrashException extends RuntimeException {
    }

    @Test
    void crashAfterHeadsAdvancedButBeforeMarkerRollsForwardOnRestart() {
        LocalObjectStore store = newStore();
        // 三个对象都有 v1 旧数据，批次覆盖它们并新增 d
        store.putBatch("init", "docs",
                List.of(item("a", "a-old"), item("b", "b-old"), item("c", "c-old")), "bob");

        store.setBatchPreCommitHookForTesting(() -> {
            throw new SimulatedCrashException();
        });
        assertThrows(SimulatedCrashException.class, () ->
                store.putBatch("gen1", "docs",
                        List.of(item("a", "a-new"), item("b", "b-new"),
                                item("c", "c-new"), item("d", "d-new")), "bob"));
        log.info("crash input: batchId=gen1 killed after HEADs advanced, before commit marker");

        // 崩溃现场：日志与 v2 元数据存在、HEAD 已推进、marker 缺失；旧实例读者只见旧版本
        assertEquals("a-old", new String(store.get("docs", "a", "carol"), StandardCharsets.UTF_8));
        assertTrue(Files.exists(root.resolve("batches/gen1/journal.json")));
        assertFalse(Files.exists(root.resolve("batches/gen1/committed")));

        // 重启：恢复逻辑前滚该批次，结果必须是整批成功、完整可读
        LocalObjectStore restarted = newStore();
        for (String k : List.of("a", "b", "c", "d")) {
            String actual = new String(restarted.get("docs", k, "carol"), StandardCharsets.UTF_8);
            log.info("rollforward decision basis: docs/{} visible after restart = {}", k, actual);
            assertEquals(k + "-new", actual, "whole batch must be readable after restart for key " + k);
        }
        assertEquals(List.of(1L, 2L),
                restarted.listVersions("docs", "a", "carol").stream()
                        .map(v -> v.version()).toList());
        assertTrue(Files.exists(root.resolve("batches/gen1/committed")),
                "recovery must finish the batch with a commit marker");

        // 同标识重试返回稳定一致的结果（不重复生成版本）
        BatchResult retry = restarted.putBatch("gen1", "docs",
                List.of(item("a", "a-new"), item("b", "b-new"),
                        item("c", "c-new"), item("d", "d-new")), "bob");
        log.info("post-restart retry of gen1 versions: {}",
                retry.versions().stream().map(v -> v.key() + "=v" + v.version()).toList());
        assertEquals(List.of(2L, 2L, 2L, 1L),
                retry.versions().stream().map(v -> v.version()).toList());
    }

    @Test
    void crashAfterMarkerPublishedIsFullyCommittedAfterRestart() {
        LocalObjectStore store = newStore();
        store.putBatch("init", "docs", List.of(item("a", "a-old")), "bob");
        store.setBatchPostCommitHookForTesting(() -> {
            throw new SimulatedCrashException();
        });
        assertThrows(SimulatedCrashException.class, () ->
                store.putBatch("visible", "docs",
                        List.of(item("a", "a-new"), item("b", "b-new")), "bob"));
        log.info("crash input: batchId=visible killed right after commit marker published");

        LocalObjectStore restarted = newStore();
        assertEquals("a-new", new String(restarted.get("docs", "a", "carol"), StandardCharsets.UTF_8));
        assertEquals("b-new", new String(restarted.get("docs", "b", "carol"), StandardCharsets.UTF_8));
        log.info("post-marker-crash decision basis: whole batch visible and complete after restart");
    }

    @Test
    void crashBeforeDurableJournalCountsAsNothingCommitted() throws Exception {
        LocalObjectStore store = newStore();
        store.putBatch("init", "docs", List.of(item("a", "a-old")), "bob");

        // 崩溃点：净新增内容块已落盘，但 PREPARED 日志尚不存在
        store.setBatchPreJournalHookForTesting(() -> {
            throw new SimulatedCrashException();
        });
        assertThrows(SimulatedCrashException.class, () ->
                store.putBatch("orphan", "docs",
                        List.of(item("a", "a-x"), item("q", "q1")), "bob"));
        log.info("crash input: batchId=orphan killed after blobs durable, before journal written");

        // 崩溃现场：无日志目录、HEAD 未前进、无版本元数据
        assertFalse(Files.exists(root.resolve("batches/orphan")),
                "no journal may exist before the durable prepare point");
        assertEquals("1", Files.readString(root.resolve("buckets/docs/objects/a/HEAD")).trim());

        // 重启：整批完全不算数，状态停留在崩溃前；孤儿内容块留待 GC
        LocalObjectStore restarted = newStore();
        assertEquals("a-old", new String(restarted.get("docs", "a", "carol"), StandardCharsets.UTF_8));
        assertThrows(StoreException.class, () -> restarted.get("docs", "q", "carol"));
        assertEquals(1, restarted.headVersion("docs", "a", "carol"));

        // 同一标识重试：此前从未到达 PREPARED，因此作为全新提交正常签发版本
        BatchResult retry = restarted.putBatch("orphan", "docs",
                List.of(item("a", "a-x"), item("q", "q1")), "bob");
        log.info("pre-journal retry decision basis: fresh commit versions = {}",
                retry.versions().stream().map(v -> v.key() + "=v" + v.version()).toList());
        assertEquals(2, retry.versions().get(0).version());
        assertEquals(1, retry.versions().get(1).version());
    }
}
