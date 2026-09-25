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
 * 版本化写入、内容去重、校验和/空写入/容量拒绝原因的可区分性，
 * 以及默认版本语义（expectedVersion=-1：创建或覆盖最新版本）。
 */
class VersioningAndDedupTest {

    private static final Logger log = LoggerFactory.getLogger(VersioningAndDedupTest.class);
    private static final String SPACE = "vspace";

    private PutRequest req(String objectId, byte[] content, long expectedVersion) {
        return new PutRequest(SPACE, objectId, OWNER, content, expectedVersion,
                BlobStore.sha256(content), 0, false);
    }

    @Test
    void versionsAreMonotonicAndReadsStayConsistent(@TempDir Path tmp) {
        var env = newEnv(tmp);
        defaultSpace(env, SPACE, 10_000);

        byte[] c1 = "hello".getBytes(StandardCharsets.UTF_8);
        byte[] c2 = "hello-world".getBytes(StandardCharsets.UTF_8);

        PutResult r1 = env.store().put(req("doc", c1, -1));
        PutResult r2 = env.store().put(req("doc", c2, -1));
        log.info("关键输入: object=doc, c1.sha={} v1={}, c2.sha={} v2={}",
                r1.sha256(), r1.version(), r2.sha256(), r2.version());

        assertEquals(1L, r1.version());
        assertEquals(2L, r2.version());

        ObjectVersion head = env.store().stat(SPACE, "doc", OWNER);
        log.info("判定依据: 最新版本号={}, sha256={}", head.version(), head.sha256());
        assertEquals(2L, head.version());
        assertEquals(BlobStore.sha256(c2), head.sha256());

        assertArrayEquals(c2, env.store().get(SPACE, "doc", OWNER));
        assertArrayEquals(c1, env.store().getVersion(SPACE, "doc", 1, OWNER));
        assertArrayEquals(c2, env.store().getVersion(SPACE, "doc", 2, OWNER));
        log.info("历史版本读取一致: v1 与 v2 内容均与提交字节完全相等");
    }

    @Test
    void duplicateContentIsDeduplicatedAcrossObjects(@TempDir Path tmp) {
        var env = newEnv(tmp);
        defaultSpace(env, SPACE, 10_000);

        byte[] content = "same-bytes".getBytes(StandardCharsets.UTF_8);
        String sha = BlobStore.sha256(content);
        log.info("关键输入: 相同内容 sha256={}, size={}, 写入两个对象 docA/docB", sha, content.length);

        PutResult a = env.store().put(req("docA", content, -1));
        PutResult b = env.store().put(req("docB", content, -1));
        PutResult a2 = env.store().put(req("docA", content, -1));

        assertFalse(a.deduplicated(), "首次写入应新建内容块");
        assertTrue(b.deduplicated(), "跨对象相同内容必须去重");
        assertTrue(a2.deduplicated(), "同对象重复内容必须去重");
        assertTrue(env.blobs().exists(sha), "去重后内容块仍存在且唯一");
        assertEquals(Map.of(sha, 3), env.store().debugBlobRefs(SPACE),
                "空间内去重计数应为 3（docA 两版本 + docB 一版本）");
        log.info("去重判定: a.dedup={}, b.dedup={}, a2.dedup={}, 空间唯一块数=1",
                a.deduplicated(), b.deduplicated(), a2.deduplicated());
    }

    @Test
    void rejectionReasonsAreDistinguishable(@TempDir Path tmp) {
        var env = newEnv(tmp, 4);
        defaultSpace(env, SPACE, 5);

        // 1) 空写入
        StoreException empty = assertThrows(StoreException.class,
                () -> env.store().put(new PutRequest(SPACE, "o", OWNER, new byte[0],
                        -1, BlobStore.sha256(new byte[0]), 0, false)));
        assertEquals(RejectReason.EMPTY_CONTENT, empty.getReason());
        log.info("拒绝判定1: 空内容 -> {}", empty.getReason());

        // 2) 校验和不匹配
        byte[] c = "abcd".getBytes(StandardCharsets.UTF_8);
        StoreException checksum = assertThrows(StoreException.class,
                () -> env.store().put(new PutRequest(SPACE, "o", OWNER, c,
                        -1, "deadbeef", 0, false)));
        assertEquals(RejectReason.CHECKSUM_MISMATCH, checksum.getReason());
        log.info("拒绝判定2: 声明校验和=deadbeef, 实际={} -> {}",
                BlobStore.sha256(c), checksum.getReason());

        // 3) 单对象超上限（上限 4 字节）
        byte[] big = "abcdef".getBytes(StandardCharsets.UTF_8);
        StoreException tooLarge = assertThrows(StoreException.class,
                () -> env.store().put(req("big", big, -1)));
        assertEquals(RejectReason.OBJECT_TOO_LARGE, tooLarge.getReason());
        log.info("拒绝判定3: size={} > maxObjectBytes=4 -> {}", big.length, tooLarge.getReason());

        // 4) 超空间配额（配额 5 字节；写入 4 字节后，不同内容再写 4 字节会超出）
        env.store().put(req("used", "abcd".getBytes(StandardCharsets.UTF_8), -1));
        StoreException quota = assertThrows(StoreException.class,
                () -> env.store().put(req("more", "efgh".getBytes(StandardCharsets.UTF_8), -1)));
        assertEquals(RejectReason.QUOTA_EXCEEDED, quota.getReason());
        log.info("拒绝判定4: 已用4字节+新增4字节 > quota=5 -> {}", quota.getReason());
    }

    @Test
    void optimisticVersionConditionsConflictAndDefaultStable(@TempDir Path tmp) {
        var env = newEnv(tmp);
        defaultSpace(env, SPACE, 10_000);

        // expectedVersion=0：对象必须不存在
        byte[] c1 = "one".getBytes(StandardCharsets.UTF_8);
        assertEquals(1L, env.store().put(req("o", c1, 0)).version());
        StoreException mustNotExist = assertThrows(StoreException.class,
                () -> env.store().put(req("o", c1, 0)));
        assertEquals(RejectReason.VERSION_CONFLICT, mustNotExist.getReason());
        log.info("判定: expectedVersion=0 在对象存在时 -> {}", mustNotExist.getReason());

        // 错误基线冲突
        byte[] c2 = "two".getBytes(StandardCharsets.UTF_8);
        StoreException stale = assertThrows(StoreException.class,
                () -> env.store().put(req("o", c2, 5L)));
        assertEquals(RejectReason.VERSION_CONFLICT, stale.getReason());
        log.info("判定: 期望基线v5, 实际最新v1 -> {}", stale.getReason());

        // 正确基线成功；默认 -1 始终得到稳定的"追加为最新版本"语义
        assertEquals(2L, env.store().put(req("o", c2, 1L)).version());
        byte[] c3 = "three".getBytes(StandardCharsets.UTF_8);
        PutResult def = env.store().put(req("o", c3, -1));
        PutResult defAgain = env.store().put(req("o", c3, -1));
        assertEquals(3L, def.version());
        assertEquals(4L, defAgain.version());
        log.info("默认语义判定: expectedVersion=-1 连续提交得到 v3、v4，单调递增且确定");
    }
}
