
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
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 版本化写入、内容去重与各类可区分拒绝的测试。
 */
class VersionedWriteTest {

    private static final Logger log = LoggerFactory.getLogger(VersionedWriteTest.class);
    private static final String ADMIN = "alice";

    @TempDir
    Path root;

    LocalObjectStore store;

    @BeforeEach
    void setUp() {
        LocalAccessControl acl = new LocalAccessControl();
        store = new LocalObjectStore(root, 1_000_000, acl);
        store.createBucket("docs", ADMIN);
        acl.grant("docs", "bob", Role.WRITER);
        acl.grant("docs", "carol", Role.READER);
    }

    private static String sha256(byte[] content) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void dedupSameContentAcrossKeysAndVersions() throws Exception {
        byte[] content = "shared-content".getBytes(StandardCharsets.UTF_8);
        VersionInfo v1 = store.put("docs", "a.txt", WriteRequest.of(content), "bob");
        VersionInfo v2 = store.put("docs", "b.txt", WriteRequest.of(content), "bob");
        VersionInfo v3 = store.put("docs", "a.txt", WriteRequest.of(content), "bob");
        log.info("dedup input: 3 puts of identical {} bytes; versions: a.txt v{}, b.txt v{}, a.txt v{}",
                content.length, v1.version(), v2.version(), v3.version());

        assertEquals(1, v1.version());
        assertEquals(1, v2.version());
        assertEquals(2, v3.version());
        assertEquals(v1.sha256(), v2.sha256());
        assertEquals(v1.sha256(), v3.sha256());

        long blobCount;
        try (var s = java.nio.file.Files.list(root.resolve("blobs"))) {
            blobCount = s.filter(p -> !p.getFileName().toString().endsWith(".tmp")).count();
        }
        log.info("dedup decision basis: blob files on disk = {} (expect 1)", blobCount);
        assertEquals(1, blobCount, "identical content must be stored only once");
    }

    @Test
    void versionsAreMonotonicAndImmutable() {
        VersionInfo v1 = store.put("docs", "f", WriteRequest.of("one".getBytes()), "bob");
        VersionInfo v2 = store.put("docs", "f", WriteRequest.of("two".getBytes()), "bob");
        log.info("monotonic input: two puts; committed versions {} -> {}", v1.version(), v2.version());
        assertEquals(1, v1.version());
        assertEquals(2, v2.version());
        assertArrayEquals("one".getBytes(), store.get("docs", "f", 1, "carol"));
        assertArrayEquals("two".getBytes(), store.get("docs", "f", "carol"));
        List<VersionInfo> versions = store.listVersions("docs", "f", "carol");
        log.info("monotonic decision basis: listed versions = {}",
                versions.stream().map(VersionInfo::version).toList());
        assertEquals(List.of(1L, 2L), versions.stream().map(VersionInfo::version).toList());
    }

    @Test
    void defaultSemanticsWithoutVersionConditionIsUnconditionalAppend() {
        VersionInfo v1 = store.put("docs", "g", WriteRequest.of("x".getBytes()), "bob");
        VersionInfo v2 = store.put("docs", "g", new WriteRequest("y".getBytes(), null, null), "bob");
        log.info("default-semantics input: two puts without expectedVersion; got v{} then v{}",
                v1.version(), v2.version());
        assertEquals(1, v1.version());
        assertEquals(2, v2.version(), "no version condition must deterministically append next version");
    }

    @Test
    void expectedVersionConflictIsRejected() {
        store.put("docs", "h", WriteRequest.of("x".getBytes()), "bob");
        StoreException ex = assertThrows(StoreException.class, () ->
                store.put("docs", "h", new WriteRequest("y".getBytes(), 5L, null), "bob"));
        log.info("version-conflict input: expectedVersion=5, actual head=1; rejection={}", ex.code());
        assertEquals(ErrorCode.VERSION_CONFLICT, ex.code());

        VersionInfo ok = store.put("docs", "h", new WriteRequest("y".getBytes(), 1L, null), "bob");
        log.info("version-conflict retry with expectedVersion=1 committed as v{}", ok.version());
        assertEquals(2, ok.version());
    }

    @Test
    void checksumMismatchIsRejected() {
        byte[] content = "payload".getBytes();
        String wrong = "0".repeat(64);
        StoreException ex = assertThrows(StoreException.class, () ->
                store.put("docs", "c", new WriteRequest(content, null, wrong), "bob"));
        log.info("checksum input: declared={} actual={}; rejection={}", wrong, sha256(content), ex.code());
        assertEquals(ErrorCode.CHECKSUM_MISMATCH, ex.code());
    }

    @Test
    void emptyWriteIsRejected() {
        StoreException ex = assertThrows(StoreException.class, () ->
                store.put("docs", "e", WriteRequest.of(new byte[0]), "bob"));
        log.info("empty-write input: 0 bytes; rejection={}", ex.code());
        assertEquals(ErrorCode.EMPTY_CONTENT, ex.code());
    }

    @Test
    void quotaExceededIsRejectedButDedupDoesNotConsumeQuota() {
        LocalObjectStore small = new LocalObjectStore(root.resolve("small"), 100, new LocalAccessControl());
        small.createBucket("b", ADMIN);
        // 直接授予写权限
        byte[] first = new byte[60]; // 全零
        byte[] second = new byte[60];
        java.util.Arrays.fill(second, (byte) 1); // 与 first 不同的内容
        small.put("b", "k1", WriteRequest.of(first), ADMIN);
        StoreException ex = assertThrows(StoreException.class, () ->
                small.put("b", "k2", WriteRequest.of(second), ADMIN));
        log.info("quota input: quota=100, used=60, new=60; rejection={}", ex.code());
        assertEquals(ErrorCode.QUOTA_EXCEEDED, ex.code());

        // 相同内容去重：不新增占用，应成功
        byte[] same = first; // 与 k1 相同的全零内容
        VersionInfo info = small.put("b", "k3", WriteRequest.of(same), ADMIN);
        log.info("quota dedup decision basis: identical content committed as v{} without new bytes",
                info.version());
        assertEquals(1, info.version());
    }
}
