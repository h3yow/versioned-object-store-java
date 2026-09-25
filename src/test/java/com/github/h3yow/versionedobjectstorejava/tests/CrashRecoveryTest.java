package com.github.h3yow.versionedobjectstorejava.tests;

import com.github.h3yow.versionedobjectstorejava.store.BlobStore;
import com.github.h3yow.versionedobjectstorejava.store.ObjectStore;
import com.github.h3yow.versionedobjectstorejava.store.ObjectVersion;
import com.github.h3yow.versionedobjectstorejava.store.PutRequest;
import com.github.h3yow.versionedobjectstorejava.store.RejectReason;
import com.github.h3yow.versionedobjectstorejava.store.StoreException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static com.github.h3yow.versionedobjectstorejava.tests.StoreTestSupport.OWNER;
import static com.github.h3yow.versionedobjectstorejava.tests.StoreTestSupport.defaultSpace;
import static com.github.h3yow.versionedobjectstorejava.tests.StoreTestSupport.newEnv;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 提交过程中进程异常退出后的崩溃恢复：
 * <ul>
 *   <li>崩溃点A（内容已落盘、日志未提交）：重启后提交不可见，孤儿内容块被回收，磁盘不泄漏；</li>
 *   <li>崩溃点B（日志已 fsync、内存更新未完成即 halt）：重启后提交完整可见、内容可读；</li>
 *   <li>并手工构造日志撕裂尾行，验证截断修复与索引一致性。</li>
 * </ul>
 *
 * <p>崩溃通过在 fork 出的子 JVM 中设置 {@code -Dobjectstore.crashpoint=...} 触发
 * {@code Runtime.halt(1)} 模拟真实进程猝死（不执行 shutdown hook）。
 */
class CrashRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(CrashRecoveryTest.class);
    private static final String SPACE = "kspace";

    /** 子进程入口：建空间 + 提交两个版本 + 提交触发崩溃的第三个版本。 */
    public static void main(String[] args) {
        Path root = Path.of(args[0]);
        ObjectStore.CrashPoint point = ObjectStore.CrashPoint.valueOf(args[1]);
        BlobStore blobs = new BlobStore(root);
        com.github.h3yow.versionedobjectstorejava.store.Journal journal =
                new com.github.h3yow.versionedobjectstorejava.store.Journal(root);
        ObjectStore store = new ObjectStore(blobs, journal);
        store.createSpace(SPACE, OWNER, 10_000, Map.of());
        byte[] c1 = "committed-1".getBytes(StandardCharsets.UTF_8);
        byte[] c2 = "committed-2".getBytes(StandardCharsets.UTF_8);
        store.put(new PutRequest(SPACE, "doc", OWNER, c1, -1, BlobStore.sha256(c1), 0, false));
        store.put(new PutRequest(SPACE, "doc", OWNER, c2, -1, BlobStore.sha256(c2), 0, false));

        byte[] c3 = ("crashing-" + point).getBytes(StandardCharsets.UTF_8);
        String sha3 = BlobStore.sha256(c3);
        store.armCrashForNextCommit(point, sha3);
        store.put(new PutRequest(SPACE, "doc", OWNER, c3, -1, sha3, 0, false));
    }

    private int runCrashProcess(Path root, ObjectStore.CrashPoint point) throws Exception {
        String cp = System.getProperty("java.class.path");
        ProcessBuilder pb = new ProcessBuilder(
                System.getProperty("java.home") + File.separator + "bin" + File.separator + "java",
                "-cp", cp,
                CrashRecoveryTest.class.getName(),
                root.toString(), point.name());
        pb.redirectErrorStream(true);
        Process p = pb.start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        int exit = p.waitFor();
        log.info("崩溃子进程结束: point={}, exit={}, 关键日志片段:\n{}",
                point, exit, tail(out, 12));
        return exit;
    }

    private static String tail(String text, int lines) {
        String[] all = text.lines().toArray(String[]::new);
        int from = Math.max(0, all.length - lines);
        return String.join("\n", java.util.Arrays.copyOfRange(all, from, all.length));
    }

    @Test
    void crashAfterBlobPutRollsBackOrphanBlobOnRestart(@TempDir Path tmp) throws Exception {
        byte[] c3 = ("crashing-" + ObjectStore.CrashPoint.AFTER_BLOB_PUT).getBytes(StandardCharsets.UTF_8);
        String sha3 = BlobStore.sha256(c3);

        int exit = runCrashProcess(tmp, ObjectStore.CrashPoint.AFTER_BLOB_PUT);
        assertEquals(1, exit, "子进程必须以 halt(1) 异常退出");
        log.info("关键输入: 崩溃点=AFTER_BLOB_PUT, 未完成版本内容 sha256={}", sha3);

        // 重启恢复
        var env = newEnv(tmp);
        ObjectVersion head = env.store().stat(SPACE, "doc", OWNER);
        assertEquals(2L, head.version(), "未提交写入不得可见，head 必须仍为 v2");
        assertArrayEquals("committed-2".getBytes(StandardCharsets.UTF_8),
                env.store().get(SPACE, "doc", OWNER));
        assertFalse(env.blobs().exists(sha3), "孤儿内容块必须在恢复时被物理回收");
        assertEquals(2, env.blobs().listBlobIds().size(), "磁盘上只剩 v1/v2 两个唯一块");
        log.info("判定依据: 恢复后 head=v2, sha256={}, 孤儿块{}存在=false, 块总数=2",
                head.sha256(), sha3);

        // 同一内容可以重新提交为 v3，说明索引与内容恢复到同一已提交状态
        var r = env.store().put(new PutRequest(SPACE, "doc", OWNER, c3, -1, sha3, 0, false));
        assertEquals(3L, r.version());
        log.info("重启后重新提交成功得到 v3，版本号严格延续");
    }

    @Test
    void crashAfterJournalAppendRecoversCommittedVersion(@TempDir Path tmp) throws Exception {
        byte[] c3 = ("crashing-" + ObjectStore.CrashPoint.AFTER_JOURNAL_APPEND).getBytes(StandardCharsets.UTF_8);
        String sha3 = BlobStore.sha256(c3);

        int exit = runCrashProcess(tmp, ObjectStore.CrashPoint.AFTER_JOURNAL_APPEND);
        assertEquals(1, exit);
        log.info("关键输入: 崩溃点=AFTER_JOURNAL_APPEND, 已提交版本内容 sha256={}", sha3);

        var env = newEnv(tmp);
        ObjectVersion head = env.store().stat(SPACE, "doc", OWNER);
        assertEquals(3L, head.version(), "日志已提交的版本必须恢复可见");
        assertArrayEquals(c3, env.store().get(SPACE, "doc", OWNER));
        assertTrue(env.blobs().exists(sha3));
        assertEquals(3, env.blobs().listBlobIds().size());
        log.info("判定依据: 恢复后 head=v3 且内容可读，块总数=3，索引与内容一致");
    }

    @Test
    void tornJournalTailLineIsTruncatedAndStateStaysConsistent(@TempDir Path tmp) throws Exception {
        var env = newEnv(tmp);
        defaultSpace(env, SPACE, 10_000);
        byte[] c1 = "ok".getBytes(StandardCharsets.UTF_8);
        env.store().put(new PutRequest(SPACE, "doc", OWNER, c1, -1, BlobStore.sha256(c1), 0, false));

        // 手工向日志追加一行没有结束换行、且 JSON 不完整的"撕裂行"
        Path journal = tmp.resolve("journals").resolve(SPACE + ".journal");
        Files.writeString(journal,
                "{\"type\":\"VersionCommitted\",\"seq\":999,\"space\":\"" + SPACE
                        + "\",\"objectId\":\"doc\",\"version\":2,\"sha256\":\"ab",
                java.nio.file.StandardOpenOption.APPEND);
        long tornLen = "{\"type\":\"VersionCommitted\",\"seq\":999,\"space\":\"".length()
                + SPACE.length() + "\",\"objectId\":\"doc\",\"version\":2,\"sha256\":\"ab".length();
        log.info("关键输入: 向日志追加 {} 字节无换行撕裂尾行后重启", tornLen);

        // 新实例重放时必须截断撕裂行，已提交 v1 保持可见，不存在 v2
        var recovered = newEnv(tmp);
        ObjectVersion head = recovered.store().stat(SPACE, "doc", OWNER);
        assertEquals(1L, head.version());
        assertArrayEquals(c1, recovered.store().get(SPACE, "doc", OWNER));
        StoreException missing = assertThrows(StoreException.class,
                () -> recovered.store().statVersion(SPACE, "doc", 2, OWNER));
        assertEquals(RejectReason.VERSION_NOT_FOUND, missing.getReason());
        String tail = Files.readString(journal).trim();
        assertTrue(tail.endsWith("}") && tail.lines().allMatch(l -> l.isBlank() || l.startsWith("{")),
                "撕裂行必须已被截断");
        log.info("判定依据: 重放后 head=v1, v2 -> VERSION_NOT_FOUND, 日志尾行已修复");
    }
}
