
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 提交过程中崩溃并重启后的恢复测试。
 *
 * <p>通过 {@link LocalObjectStore#setCommitHookForTesting} 在“版本元数据已持久化、
 * HEAD 尚未更新”的崩溃点注入异常来模拟进程崩溃；随后在同一目录上构造新的
 * store 实例模拟进程重启，验证恢复语义。</p>
 */
class CrashRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(CrashRecoveryTest.class);
    private static final String ADMIN = "alice";

    @TempDir
    Path root;

    private LocalObjectStore newStore() {
        LocalAccessControl acl = new LocalAccessControl();
        LocalObjectStore s = new LocalObjectStore(root, 10_000_000, acl);
        s.createBucket("r", ADMIN);
        acl.grant("r", "writer", Role.WRITER);
        acl.grant("r", "reader", Role.READER);
        return s;
    }

    @Test
    void crashBetweenMetadataAndHeadIsRolledBackOnRestart() {
        LocalObjectStore store = newStore();
        VersionInfo v1 = store.put("r", "obj", WriteRequest.of("committed".getBytes()), "writer");
        log.info("crash-test setup: committed v{} of r/obj", v1.version());

        // 模拟崩溃：v2 元数据落盘后、HEAD 更新前进程退出
        store.setCommitHookForTesting(() -> {
            throw new SimulatedCrashException();
        });
        assertThrows(SimulatedCrashException.class, () ->
                store.put("r", "obj", WriteRequest.of("uncommitted".getBytes()), "writer"));
        log.info("crash-test: simulated crash after v2 metadata durable, before HEAD update");

        // 崩溃现场应存在悬空的 v-2.json（未提交）
        Path objDir = root.resolve("buckets/r/objects/obj");
        assertTrue(Files.exists(objDir.resolve("versions/v-2.json")),
                "dangling v-2.json should exist at crash site");

        // 重启：新实例在构造时执行恢复
        LocalObjectStore restarted = newStore();
        long head = restarted.headVersion("r", "obj", "reader");
        List<VersionInfo> versions = restarted.listVersions("r", "obj", "reader");
        log.info("crash-test decision basis after restart: head={}, versions={}",
                head, versions.stream().map(VersionInfo::version).toList());

        assertEquals(1, head, "uncommitted v2 must not become visible");
        assertEquals(1, versions.size());
        assertArrayEquals("committed".getBytes(StandardCharsets.UTF_8),
                restarted.get("r", "obj", "reader"));
        assertFalse(Files.exists(objDir.resolve("versions/v-2.json")),
                "recovery must remove dangling version file");
        assertFalse(Files.exists(objDir.resolve("HEAD.tmp")),
                "recovery must remove temp files");
    }

    @Test
    void committedStateSurvivesRestart() {
        LocalObjectStore store = newStore();
        VersionInfo v1 = store.put("r", "a", WriteRequest.of("one".getBytes()), "writer");
        VersionInfo v2 = store.put("r", "a", WriteRequest.of("two".getBytes()), "writer");
        VersionInfo vb = store.put("r", "b", WriteRequest.of("one".getBytes()), "writer"); // 与 v1 去重
        log.info("restart-test setup: r/a v{},v{}; r/b v{} (dedup sha={})",
                v1.version(), v2.version(), vb.version(), v1.sha256());

        LocalObjectStore restarted = newStore();
        assertEquals(2, restarted.headVersion("r", "a", "reader"));
        assertArrayEquals("one".getBytes(), restarted.get("r", "a", 1, "reader"));
        assertArrayEquals("two".getBytes(), restarted.get("r", "a", "reader"));
        assertArrayEquals("one".getBytes(), restarted.get("r", "b", "reader"));

        // 去重在重启后仍然成立：相同内容只占一个块
        try (var s = Files.list(root.resolve("blobs"))) {
            long blobs = s.filter(p -> !p.getFileName().toString().endsWith(".tmp")).count();
            log.info("restart-test decision basis: blob count after restart = {} (expect 2)", blobs);
            assertEquals(2, blobs);
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void commitAfterCrashReusesNoVersionNumber() {
        LocalObjectStore store = newStore();
        store.put("r", "seq", WriteRequest.of("v1".getBytes()), "writer");
        store.setCommitHookForTesting(() -> {
            throw new SimulatedCrashException();
        });
        assertThrows(SimulatedCrashException.class, () ->
                store.put("r", "seq", WriteRequest.of("v2-crashed".getBytes()), "writer"));

        LocalObjectStore restarted = newStore();
        VersionInfo v = restarted.put("r", "seq", WriteRequest.of("v2-retried".getBytes()), "writer");
        log.info("crash-retry decision basis: retried commit got v{} (crashed attempt also used v2)",
                v.version());
        // 崩溃的 v2 从未提交，重试重新签发 v2 是确定且一致的（HEAD 从未前进到 2）
        assertEquals(2, v.version());
        assertArrayEquals("v2-retried".getBytes(), restarted.get("r", "seq", "reader"));
    }

    /** 模拟进程崩溃的标记异常。 */
    private static final class SimulatedCrashException extends RuntimeException {
    }
}
