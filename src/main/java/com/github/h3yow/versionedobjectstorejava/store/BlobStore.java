package com.github.h3yow.versionedobjectstorejava.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * 内容块存储：按内容 SHA-256 寻址。
 *
 * <p>磁盘布局（根目录由 Spring 配置注入）：
 * <pre>
 *   blobs/ab/cdef....bin        不可变内容块（哈希前两位分片）
 *   blobs/ab/cdef....bin.refs   该块的存活引用计数（与块文件同生共死）
 *   tmp/*.tmp                   崩溃后可安全删除的临时写入文件
 * </pre>
 *
 * <p>写入采用"临时文件 + fsync + 原子 move"，块文件一旦出现即完整；
 * 引用计数文件与块文件的存在性共同保证去重正确性。
 */
public class BlobStore {

    private static final Logger log = LoggerFactory.getLogger(BlobStore.class);
    private static final int SHARD_LEN = 2;

    private final Path blobsDir;
    private final Path tmpDir;

    public BlobStore(Path rootDir) {
        this.blobsDir = rootDir.resolve("blobs");
        this.tmpDir = rootDir.resolve("tmp");
        try {
            Files.createDirectories(blobsDir);
            Files.createDirectories(tmpDir);
        } catch (IOException e) {
            throw new IllegalStateException("无法初始化内容块目录: " + rootDir, e);
        }
    }

    /** 计算 SHA-256 十六进制小写摘要。 */
    public static String sha256(byte[] content) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(content));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 不可用", e);
        }
    }

    /**
     * 将内容以临时文件原子落盘并按校验和命名；块已存在则直接复用（去重，不新增磁盘占用）。
     *
     * @return true 表示新建了块；false 表示复用了已有块
     */
    public boolean putIfAbsent(String sha256, byte[] content) throws IOException {
        Path target = blobPath(sha256);
        if (Files.exists(target)) {
            log.info("内容块去重命中: sha256={}, size={}", sha256, content.length);
            return false;
        }
        Path tmp = tmpDir.resolve(sha256 + "-" + Thread.currentThread().getId() + "-" + System.nanoTime() + ".tmp");
        try {
            Files.write(tmp, content, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            // 强制内容落盘，崩溃时 move 之后块内容必须完整
            try (var ch = FileChannel.open(tmp, StandardOpenOption.READ)) {
                ch.force(true);
            }
            Files.createDirectories(target.getParent());
            try {
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicUnsupported) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
            // 新建块初始无引用，引用数由提交方在日志落盘后递增
            Files.writeString(refsPath(sha256), "0", StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            fsyncDirectory(target.getParent());
            fsyncDirectory(blobsDir);
            log.info("内容块新建: sha256={}, size={}", sha256, content.length);
            return true;
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    /** 读取内容块；块缺失（被回收或从未提交）抛 OBJECT_NOT_FOUND。 */
    public byte[] get(String sha256) throws IOException {
        Path target = blobPath(sha256);
        if (!Files.exists(target)) {
            throw new StoreException(RejectReason.OBJECT_NOT_FOUND, "内容块缺失: " + sha256);
        }
        return Files.readAllBytes(target);
    }

    public boolean exists(String sha256) {
        return Files.exists(blobPath(sha256));
    }

    /** 引用计数 +1（新提交版本引用该块时调用）。 */
    public synchronized void incrementRefs(String sha256) throws IOException {
        int refs = readRefs(sha256);
        writeRefs(sha256, refs + 1);
        log.info("内容块引用计数递增: sha256={}, refs={}", sha256, refs + 1);
    }

    /**
     * 仅当内容块不再被任何版本引用（refs&lt;=0）时物理回收；仍被引用则抛 BLOB_IN_USE。
     */
    public synchronized void reclaimIfUnreferenced(String sha256) throws IOException {
        int refs = readRefs(sha256);
        if (refs > 0) {
            log.info("内容块仍被引用，拒绝回收: sha256={}, refs={}", sha256, refs);
            throw new StoreException(RejectReason.BLOB_IN_USE,
                    "内容块仍被 " + refs + " 个版本引用: " + sha256);
        }
        Files.deleteIfExists(blobPath(sha256));
        Files.deleteIfExists(refsPath(sha256));
        log.info("内容块已物理回收: sha256={}", sha256);
    }

    /** 引用计数 -1（删除版本时调用），返回递减后的计数；块不存在返回 -1。 */
    public synchronized int decrementRefs(String sha256) throws IOException {
        if (!exists(sha256)) {
            return -1;
        }
        int refs = Math.max(0, readRefs(sha256) - 1);
        writeRefs(sha256, refs);
        log.info("内容块引用计数递减: sha256={}, refs={}", sha256, refs);
        return refs;
    }

    /** 恢复阶段按真实存活版本数重建引用计数（权威值来自日志，不信任旧计数文件）。 */
    public synchronized void rebuildRefs(String sha256, int liveRefs) throws IOException {
        if (!exists(sha256)) {
            log.warn("重建引用计数时发现内容块缺失: sha256={}", sha256);
            return;
        }
        writeRefs(sha256, liveRefs);
    }

    /**
     * 崩溃恢复协调：liveRefs&gt;0 时以日志权威值重建计数；为 0 时说明是崩溃事务遗留的
     * 孤儿块，直接物理删除。
     */
    public synchronized void reconcile(String sha256, int liveRefs) throws IOException {
        if (liveRefs > 0) {
            rebuildRefs(sha256, liveRefs);
        } else {
            Files.deleteIfExists(blobPath(sha256));
            Files.deleteIfExists(refsPath(sha256));
            log.info("回收崩溃事务遗留的孤儿内容块: sha256={}", sha256);
        }
    }

    /** 枚举磁盘上现存的全部内容块 ID（用于恢复时扫描）。 */
    public java.util.List<String> listBlobIds() throws IOException {
        java.util.List<String> ids = new java.util.ArrayList<>();
        if (!Files.exists(blobsDir)) {
            return ids;
        }
        try (var shards = Files.newDirectoryStream(blobsDir)) {
            for (Path shard : shards) {
                if (!Files.isDirectory(shard) || shard.getFileName().toString().length() != SHARD_LEN) {
                    continue;
                }
                try (var files = Files.newDirectoryStream(shard, "*.bin")) {
                    for (Path f : files) {
                        String name = f.getFileName().toString();
                        ids.add(shard.getFileName() + name.substring(0, name.length() - ".bin".length()));
                    }
                }
            }
        }
        return ids;
    }

    /** 清理崩溃残留的临时文件。 */
    public void cleanStaging() throws IOException {
        try (var entries = Files.newDirectoryStream(tmpDir, "*.tmp")) {
            for (Path p : entries) {
                Files.deleteIfExists(p);
                log.info("清理崩溃残留临时文件: {}", p);
            }
        }
    }

    private int readRefs(String sha256) throws IOException {
        Path refs = refsPath(sha256);
        if (!Files.exists(refs)) {
            return 0;
        }
        return Integer.parseInt(Files.readString(refs, StandardCharsets.UTF_8).trim());
    }

    private void writeRefs(String sha256, int refs) throws IOException {
        Path refsFile = refsPath(sha256);
        Path tmp = tmpDir.resolve(sha256 + ".refs.tmp");
        Files.writeString(tmp, Integer.toString(refs), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        try {
            Files.move(tmp, refsFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException atomicUnsupported) {
            Files.move(tmp, refsFile, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private Path blobPath(String sha256) {
        return blobsDir.resolve(relativePath(sha256) + ".bin");
    }

    private Path refsPath(String sha256) {
        return blobsDir.resolve(relativePath(sha256) + ".bin.refs");
    }

    /** 块文件的相对路径（按哈希前两位分片）。 */
    public static String relativePath(String sha256) {
        String shard = sha256.substring(0, SHARD_LEN);
        String rest = sha256.substring(SHARD_LEN);
        return shard + "/" + rest;
    }

    private static void fsyncDirectory(Path dir) throws IOException {
        // POSIX：对目录文件描述符 fsync 才能保证新建/删除的目录项持久
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException ignored) {
            // 个别文件系统/平台不支持目录 fsync，尽力而为
        }
    }
}
