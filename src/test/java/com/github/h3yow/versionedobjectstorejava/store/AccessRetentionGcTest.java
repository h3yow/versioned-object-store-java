
package com.github.h3yow.versionedobjectstorejava.store;

import com.github.h3yow.versionedobjectstorejava.store.acl.LocalAccessControl;
import com.github.h3yow.versionedobjectstorejava.store.acl.Role;
import com.github.h3yow.versionedobjectstorejava.store.local.LocalObjectStore;
import com.github.h3yow.versionedobjectstorejava.store.model.RetentionPolicy;
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
import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 访问控制、保留期边界、合规锁定与内容回收测试。
 */
class AccessRetentionGcTest {

    private static final Logger log = LoggerFactory.getLogger(AccessRetentionGcTest.class);
    private static final String ADMIN = "alice";

    @TempDir
    Path root;

    LocalAccessControl acl;
    LocalObjectStore store;

    @BeforeEach
    void setUp() {
        acl = new LocalAccessControl();
        store = new LocalObjectStore(root, 10_000_000, acl);
        store.createBucket("sec", ADMIN);
        acl.grant("sec", "writer", Role.WRITER);
        acl.grant("sec", "reader", Role.READER);
    }

    @Test
    void unauthorizedAccessIsRejectedDistinctly() {
        store.put("sec", "doc", WriteRequest.of("data".getBytes()), "writer");

        StoreException noGrant = assertThrows(StoreException.class, () ->
                store.get("sec", "doc", "stranger"));
        log.info("acl input: principal=stranger (no grant) read; rejection={}", noGrant.code());
        assertEquals(ErrorCode.ACCESS_DENIED, noGrant.code());

        StoreException readerWrites = assertThrows(StoreException.class, () ->
                store.put("sec", "doc", WriteRequest.of("x".getBytes()), "reader"));
        log.info("acl input: principal=reader (READER) write; rejection={}", readerWrites.code());
        assertEquals(ErrorCode.ACCESS_DENIED, readerWrites.code());

        StoreException readerDeletes = assertThrows(StoreException.class, () ->
                store.deleteVersion("sec", "doc", 1, "reader"));
        log.info("acl input: principal=reader delete; rejection={}", readerDeletes.code());
        assertEquals(ErrorCode.ACCESS_DENIED, readerDeletes.code());

        // 有权限者可正常访问
        assertArrayEquals("data".getBytes(), store.get("sec", "doc", "reader"));
    }

    @Test
    void missingObjectIsRejectedDistinctly() {
        StoreException ex = assertThrows(StoreException.class, () ->
                store.get("sec", "nope", "reader"));
        log.info("not-found input: get missing object; rejection={}", ex.code());
        assertEquals(ErrorCode.OBJECT_NOT_FOUND, ex.code());

        StoreException ex2 = assertThrows(StoreException.class, () ->
                store.get("sec", "nope", 7, "reader"));
        assertEquals(ErrorCode.OBJECT_NOT_FOUND, ex2.code());
    }

    @Test
    void deleteWithinRetentionPeriodIsRejected() {
        store.setRetention("sec", "ret",
                RetentionPolicy.until(Instant.now().plus(Duration.ofHours(1))), "writer");
        VersionInfo v1 = store.put("sec", "ret", WriteRequest.of("r1".getBytes()), "writer");
        log.info("retention input: retentionUntil={} (future), delete v{}",
                v1.retentionUntil(), v1.version());

        StoreException ex = assertThrows(StoreException.class, () ->
                store.deleteVersion("sec", "ret", v1.version(), "writer"));
        log.info("retention decision basis: now is before retentionUntil; rejection={}", ex.code());
        assertEquals(ErrorCode.RETENTION_ACTIVE, ex.code());
        // 版本仍在
        assertArrayEquals("r1".getBytes(), store.get("sec", "ret", "reader"));
    }

    @Test
    void deleteAfterRetentionExpiredSucceeds() {
        // 保留期设为过去的时刻，模拟已过期边界
        store.setRetention("sec", "old",
                RetentionPolicy.until(Instant.now().minus(Duration.ofSeconds(1))), "writer");
        VersionInfo v1 = store.put("sec", "old", WriteRequest.of("gone".getBytes()), "writer");
        log.info("retention-expired input: retentionUntil={} (past), delete v{}",
                v1.retentionUntil(), v1.version());

        store.deleteVersion("sec", "old", v1.version(), "writer");
        StoreException ex = assertThrows(StoreException.class, () ->
                store.get("sec", "old", "reader"));
        log.info("retention-expired decision basis: now is after retentionUntil; "
                + "delete succeeded, subsequent read rejection={}", ex.code());
        assertEquals(ErrorCode.OBJECT_NOT_FOUND, ex.code());
    }

    @Test
    void complianceLockedVersionCannotBeDeleted() {
        store.setRetention("sec", "lock", new RetentionPolicy(null, true), "writer");
        VersionInfo v1 = store.put("sec", "lock", WriteRequest.of("locked".getBytes()), "writer");
        log.info("compliance input: complianceLock=true, delete v{}", v1.version());

        StoreException ex = assertThrows(StoreException.class, () ->
                store.deleteVersion("sec", "lock", v1.version(), "writer"));
        log.info("compliance decision basis: complianceLock=true; rejection={}", ex.code());
        assertEquals(ErrorCode.COMPLIANCE_LOCKED, ex.code());
    }

    @Test
    void blobReclaimedOnlyWhenNoVersionReferencesIt() throws Exception {
        byte[] shared = "shared-blob".getBytes(StandardCharsets.UTF_8);
        VersionInfo a1 = store.put("sec", "A", WriteRequest.of(shared), "writer");
        VersionInfo b1 = store.put("sec", "B", WriteRequest.of(shared), "writer");
        log.info("gc input: same content committed as sec/A v{} and sec/B v{}, sha={}",
                a1.version(), b1.version(), a1.sha256());

        // 删除 A 的唯一版本：内容仍被 B 引用，不得回收
        store.deleteVersion("sec", "A", a1.version(), "writer");
        long reclaimed1 = store.garbageCollect(ADMIN);
        log.info("gc decision basis: A deleted but B still references sha; reclaimed={} bytes", reclaimed1);
        assertEquals(0, reclaimed1, "blob still referenced by B must not be reclaimed");
        assertArrayEquals(shared, store.get("sec", "B", "reader"));

        // 删除 B 的唯一版本：内容不再被引用，允许回收
        store.deleteVersion("sec", "B", b1.version(), "writer");
        long reclaimed2 = store.garbageCollect(ADMIN);
        log.info("gc decision basis: no version references sha; reclaimed={} bytes", reclaimed2);
        assertEquals(shared.length, reclaimed2);
        try (var s = Files.list(root.resolve("blobs"))) {
            assertEquals(0, s.filter(p -> !p.getFileName().toString().endsWith(".tmp")).count());
        }
    }

    @Test
    void deleteMissingVersionIsRejectedDistinctly() {
        store.put("sec", "d", WriteRequest.of("x".getBytes()), "writer");
        StoreException ex = assertThrows(StoreException.class, () ->
                store.deleteVersion("sec", "d", 99, "writer"));
        log.info("delete-missing input: delete v99 of object with head=1; rejection={}", ex.code());
        assertEquals(ErrorCode.OBJECT_NOT_FOUND, ex.code());
    }
}
