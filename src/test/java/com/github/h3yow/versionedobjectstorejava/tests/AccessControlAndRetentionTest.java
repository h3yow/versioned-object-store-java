package com.github.h3yow.versionedobjectstorejava.tests;

import com.github.h3yow.versionedobjectstorejava.store.BlobStore;
import com.github.h3yow.versionedobjectstorejava.store.DeleteResult;
import com.github.h3yow.versionedobjectstorejava.store.ObjectVersion;
import com.github.h3yow.versionedobjectstorejava.store.Permission;
import com.github.h3yow.versionedobjectstorejava.store.PutRequest;
import com.github.h3yow.versionedobjectstorejava.store.RejectReason;
import com.github.h3yow.versionedobjectstorejava.store.StoreException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;

import static com.github.h3yow.versionedobjectstorejava.tests.StoreTestSupport.OUTSIDER;
import static com.github.h3yow.versionedobjectstorejava.tests.StoreTestSupport.OWNER;
import static com.github.h3yow.versionedobjectstorejava.tests.StoreTestSupport.READER;
import static com.github.h3yow.versionedobjectstorejava.tests.StoreTestSupport.newEnv;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 访问控制、对象/版本不存在、保留期与合规锁定边界，以及"最后引用解除才回收"的删除语义。
 */
class AccessControlAndRetentionTest {

    private static final Logger log = LoggerFactory.getLogger(AccessControlAndRetentionTest.class);
    private static final String SPACE = "aspace";

    private PutRequest putAs(String objectId, String principal, byte[] content,
                            long retentionMillis, boolean legalHold) {
        return new PutRequest(SPACE, objectId, principal, content, -1,
                BlobStore.sha256(content), retentionMillis, legalHold);
    }

    private StoreTestSupport.TestEnv setup(Path tmp) {
        var env = newEnv(tmp);
        env.store().createSpace(SPACE, OWNER, 10_000,
                Map.of(READER, Permission.READ, "dave", Permission.DELETE));
        byte[] c = "secret".getBytes(StandardCharsets.UTF_8);
        env.store().put(putAs("doc", OWNER, c, 0, false));
        return env;
    }

    @Test
    void accessControlAndNotFoundAreDistinguishable(
            @TempDir Path tmp) {
        var env = setup(tmp);

        StoreException outsiderWrite = assertThrows(StoreException.class,
                () -> env.store().put(putAs("x", OUTSIDER, new byte[]{1}, 0, false)));
        assertEquals(RejectReason.ACCESS_DENIED, outsiderWrite.getReason());
        log.info("判定1: 无授权主体 {} 写入 -> {}", OUTSIDER, outsiderWrite.getReason());

        StoreException readerWrite = assertThrows(StoreException.class,
                () -> env.store().put(putAs("x", READER, new byte[]{1}, 0, false)));
        assertEquals(RejectReason.ACCESS_DENIED, readerWrite.getReason());
        log.info("判定2: 只有 READ 的 {} 写入 -> {}", READER, readerWrite.getReason());

        StoreException noSpace = assertThrows(StoreException.class,
                () -> env.store().stat("nospace", "doc", OWNER));
        assertEquals(RejectReason.SPACE_NOT_FOUND, noSpace.getReason());
        log.info("判定3: 空间不存在 -> {}", noSpace.getReason());

        StoreException noObject = assertThrows(StoreException.class,
                () -> env.store().stat(SPACE, "missing", OWNER));
        assertEquals(RejectReason.OBJECT_NOT_FOUND, noObject.getReason());
        log.info("判定4: 对象不存在 -> {}", noObject.getReason());

        StoreException noVersion = assertThrows(StoreException.class,
                () -> env.store().statVersion(SPACE, "doc", 99, OWNER));
        assertEquals(RejectReason.VERSION_NOT_FOUND, noVersion.getReason());
        log.info("判定5: 版本不存在 v99 -> {}", noVersion.getReason());

        // READER 可读，DELETE 权限者可删，权限层级验证
        assertArrayEquals("secret".getBytes(StandardCharsets.UTF_8),
                env.store().get(SPACE, "doc", READER));
        DeleteResult dr = env.store().deleteVersion(SPACE, "doc", 1, "dave");
        assertTrue(dr.blobReclaimed(), "唯一版本删除后内容块必须被物理回收");
        log.info("判定6: DELETE 权限者删除成功, blobReclaimed={}", dr.blobReclaimed());
    }

    @Test
    void retentionBoundaryAndLegalHold(@TempDir Path tmp) throws Exception {
        var env = setup(tmp);
        // 再提交一个保留期版本 v2（保留 300ms）
        byte[] retained = "retained".getBytes(StandardCharsets.UTF_8);
        env.store().put(putAs("doc", OWNER, retained, 300, false));
        // v3：合规锁定
        byte[] held = "held".getBytes(StandardCharsets.UTF_8);
        env.store().put(putAs("doc", OWNER, held, 0, true));

        StoreException hold = assertThrows(StoreException.class,
                () -> env.store().deleteVersion(SPACE, "doc", 3, OWNER));
        assertEquals(RejectReason.LEGAL_HOLD_ACTIVE, hold.getReason());
        log.info("关键输入: v3 legalHold=true, 删除判定 -> {}", hold.getReason());

        StoreException withinRetention = assertThrows(StoreException.class,
                () -> env.store().deleteVersion(SPACE, "doc", 2, OWNER));
        assertEquals(RejectReason.RETENTION_ACTIVE, withinRetention.getReason());
        ObjectVersion v2 = env.store().statVersion(SPACE, "doc", 2, OWNER);
        log.info("关键输入: v2 retentionUntil={} 早于当前时刻, 删除判定 -> {}",
                v2.retentionUntilEpochMillis(), withinRetention.getReason());

        // 等待跨过保留期边界：边界之后删除必须成功
        long waitMs = v2.retentionUntilEpochMillis() - System.currentTimeMillis() + 20;
        Thread.sleep(Math.max(waitMs, 50));
        DeleteResult ok = env.store().deleteVersion(SPACE, "doc", 2, OWNER);
        log.info("保留期边界后删除 v2 成功: blobReclaimed={} (v2 是该内容唯一版本)", ok.blobReclaimed());
        assertTrue(ok.blobReclaimed());

        // 法务锁定版本无论等多久都不能删
        StoreException stillHeld = assertThrows(StoreException.class,
                () -> env.store().deleteVersion(SPACE, "doc", 3, OWNER));
        assertEquals(RejectReason.LEGAL_HOLD_ACTIVE, stillHeld.getReason());

        // 已删除版本再次删除得到 VERSION_NOT_FOUND，且不再可读取
        StoreException deletedAgain = assertThrows(StoreException.class,
                () -> env.store().deleteVersion(SPACE, "doc", 2, OWNER));
        assertEquals(RejectReason.VERSION_NOT_FOUND, deletedAgain.getReason());
        StoreException readDeleted = assertThrows(StoreException.class,
                () -> env.store().statVersion(SPACE, "doc", 2, OWNER));
        assertEquals(RejectReason.VERSION_NOT_FOUND, readDeleted.getReason());
        log.info("判定依据: v2 已删除 -> 再删/再读均 VERSION_NOT_FOUND; v3 仍被法务锁定");
    }

    @Test
    void blobIsReclaimedOnlyWhenNoVersionReferencesIt(@TempDir Path tmp) {
        var env = newEnv(tmp);
        env.store().createSpace(SPACE, OWNER, 10_000, Map.of());
        byte[] shared = "shared-content".getBytes(StandardCharsets.UTF_8);
        String sha = BlobStore.sha256(shared);

        // 两个对象共享同一内容；docA 再追加一次不同内容
        env.store().put(putAs("docA", OWNER, shared, 0, false));
        env.store().put(putAs("docB", OWNER, shared, 0, false));
        byte[] other = "other".getBytes(StandardCharsets.UTF_8);
        env.store().put(putAs("docA", OWNER, other, 0, false));

        log.info("关键输入: sha={} 被 docA#v1 与 docB#v1 引用", sha);

        // 删除 docA#v1：docB#v1 仍引用，块不得回收
        DeleteResult r1 = env.store().deleteVersion(SPACE, "docA", 1, OWNER);
        assertFalse(r1.blobReclaimed());
        assertTrue(env.blobs().exists(sha), "仍被引用时内容块必须保留");
        assertArrayEquals(shared, env.store().getVersion(SPACE, "docB", 1, OWNER),
                "存活版本读取必须不受影响");
        log.info("判定1: 删除 docA#v1 后 blobReclaimed=false, docB#v1 仍可读");

        // 删除最后引用 docB#v1：块才允许物理回收
        DeleteResult r2 = env.store().deleteVersion(SPACE, "docB", 1, OWNER);
        assertTrue(r2.blobReclaimed());
        assertFalse(env.blobs().exists(sha), "无任何版本引用后必须回收");
        log.info("判定2: 删除 docB#v1（最后引用）后 blobReclaimed=true, 块已从磁盘移除");
    }
}
