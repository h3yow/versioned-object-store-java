package com.github.h3yow.versionedobjectstorejava.store;

import com.github.h3yow.versionedobjectstorejava.store.event.Event;
import com.github.h3yow.versionedobjectstorejava.store.event.SpaceCreatedEvent;
import com.github.h3yow.versionedobjectstorejava.store.event.VersionCommittedEvent;
import com.github.h3yow.versionedobjectstorejava.store.event.VersionDeletedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * 版本化本地对象存储的核心门面。
 *
 * <p>设计要点：
 * <ul>
 *   <li>事件溯源：{@link Journal} 是唯一的已提交事实来源，内存索引可随时由日志重建；</li>
 *   <li>内容去重：内容按 SHA-256 寻址，空间级按唯一内容块计数占用；</li>
 *   <li>原子提交：全部变更在一把全局锁内串行化，"内容落盘 → 日志 fsync → 内存更新"顺序保证
 *       崩溃后日志已提交的一定可见，日志未提交的（含已落盘孤儿块）一律回滚清理；</li>
 *   <li>版本语义：同对象版本号单调递增；expectedVersion=-1（默认）= 创建或覆盖最新版本，
 *       0 = 对象必须不存在，N&gt;0 = 当前最新版本必须为 N，否则 VERSION_CONFLICT。</li>
 * </ul>
 */
public class ObjectStore {

    private static final Logger log = LoggerFactory.getLogger(ObjectStore.class);

    /** 默认乐观锁语义：未携带版本条件。 */
    public static final long DEFAULT_EXPECTED_VERSION = -1L;

    /** 崩溃注入点（仅测试用，通过系统属性 objectstore.crashpoint 指定）。 */
    public enum CrashPoint { AFTER_BLOB_PUT, AFTER_JOURNAL_APPEND }

    private final BlobStore blobs;
    private final Journal journal;
    private final long maxObjectBytes;
    private final Object lock = new Object();
    private final Map<String, SpaceState> spaces = new LinkedHashMap<>();

    public ObjectStore(BlobStore blobs, Journal journal) {
        this(blobs, journal, Long.MAX_VALUE);
    }

    public ObjectStore(BlobStore blobs, Journal journal, long maxObjectBytes) {
        this.blobs = blobs;
        this.journal = journal;
        this.maxObjectBytes = maxObjectBytes;
        recover();
    }

    private static final class SpaceState {
        String owner;
        long quotaBytes;
        Map<String, Permission> grants = new HashMap<>();
        /** objectId -> 对象（含全部版本，含已删除版本的可追溯元数据） */
        Map<String, ObjectState> objects = new LinkedHashMap<>();
        /** sha256 -> 存活版本引用数（空间内容占用的权威去重计数） */
        Map<String, Integer> blobRefs = new HashMap<>();
        long lastSeq;
    }

    private static final class ObjectState {
        TreeMap<Long, ObjectVersion> versions = new TreeMap<>();

        ObjectVersion latest() {
            return versions.isEmpty() ? null : versions.lastEntry().getValue();
        }
    }

    // ------------------------------------------------------------ 空间管理

    public void createSpace(String space, String ownerPrincipal, long quotaBytes,
                            Map<String, Permission> grants) {
        synchronized (lock) {
            if (spaces.containsKey(space)) {
                throw new StoreException(RejectReason.VERSION_CONFLICT, "存储空间已存在: " + space);
            }
            Map<String, Permission> safeGrants = new HashMap<>(grants == null ? Map.of() : grants);
            safeGrants.put(ownerPrincipal, Permission.ADMIN);
            long seq = journal.nextSeq();
            SpaceCreatedEvent event = new SpaceCreatedEvent(seq, space, ownerPrincipal, quotaBytes,
                    Map.copyOf(safeGrants));
            journal.append(space, event);
            SpaceState st = new SpaceState();
            st.owner = ownerPrincipal;
            st.quotaBytes = quotaBytes;
            st.grants = new HashMap<>(safeGrants);
            st.lastSeq = seq;
            spaces.put(space, st);
            log.info("存储空间已创建: space={}, owner={}, quotaBytes={}, grants={}",
                    space, ownerPrincipal, quotaBytes, safeGrants);
        }
    }

    // ------------------------------------------------------------ 写入提交

    public PutResult put(PutRequest req) {
        Objects.requireNonNull(req, "req");
        String shaActual = req.content() == null ? null : BlobStore.sha256(req.content());
        log.info("提交请求: space={}, object={}, principal={}, size={}, expectedVersion={}, "
                        + "declaredSha256={}, actualSha256={}, retentionMillis={}, legalHold={}",
                req.space(), req.objectId(), req.principal(),
                req.content() == null ? 0 : req.content().length,
                req.expectedVersion(), req.expectedSha256(), shaActual,
                req.retentionMillis(), req.legalHold());

        // ---- 可区分的输入校验（顺序固定，便于判定原因稳定）----
        if (req.content() == null || req.content().length == 0) {
            throw reject(RejectReason.EMPTY_CONTENT, "拒绝空写入", req);
        }
        if (req.expectedSha256() == null || !req.expectedSha256().equalsIgnoreCase(shaActual)) {
            throw reject(RejectReason.CHECKSUM_MISMATCH,
                    "校验和不匹配: declared=" + req.expectedSha256() + ", actual=" + shaActual, req);
        }
        if (req.content().length > maxObjectBytes) {
            throw reject(RejectReason.OBJECT_TOO_LARGE,
                    "对象大小 " + req.content().length + " 超过上限 " + maxObjectBytes, req);
        }

        synchronized (lock) {
            SpaceState space = requireSpace(req.space());
            AccessControl.checkSpace(space.grants, req.principal(), Permission.WRITE);

            ObjectState obj = space.objects.get(req.objectId());
            ObjectVersion head = obj == null ? null : obj.latest();
            long expected = req.expectedVersion();
            long actualHead = head == null ? 0L : head.version();
            if (expected != DEFAULT_EXPECTED_VERSION && expected != actualHead) {
                log.warn("版本冲突: object={}/{}, expectedVersion={}, actualHead={}",
                        req.space(), req.objectId(), expected, actualHead);
                throw new StoreException(RejectReason.VERSION_CONFLICT,
                        "版本冲突: 期望基线=" + expected + ", 实际最新=" + actualHead);
            }

            boolean contentExists = blobs.exists(shaActual);
            long currentUsage = contentExists && space.blobRefs.containsKey(shaActual)
                    ? 0L : req.content().length;
            long usedBytes = usedBytes(space);
            if (usedBytes + currentUsage > space.quotaBytes) {
                log.warn("超出空间容量: space={}, used={}, delta={}, quota={}",
                        space, usedBytes, currentUsage, space.quotaBytes);
                throw new StoreException(RejectReason.QUOTA_EXCEEDED,
                        "超出空间容量限制: used=" + usedBytes + ", delta=" + currentUsage
                                + ", quota=" + space.quotaBytes);
            }

            boolean createdBlob;
            try {
                createdBlob = blobs.putIfAbsent(shaActual, req.content());
            } catch (IOException e) {
                throw new UncheckedIOException("内容落盘失败", e);
            }
            maybeCrash(CrashPoint.AFTER_BLOB_PUT, req, shaActual);

            // ---- 阶段2：事件日志追加并 fsync（提交点）----
            long newVersion = actualHead + 1;
            long now = System.currentTimeMillis();
            long retentionUntil = req.retentionMillis() > 0 ? now + req.retentionMillis() : 0L;
            long seq = journal.nextSeq();
            VersionCommittedEvent event = new VersionCommittedEvent(
                    seq, req.space(), req.objectId(), newVersion, shaActual, req.content().length,
                    expected, !createdBlob, req.principal(), retentionUntil, req.legalHold(), now);
            journal.append(req.space(), event);
            maybeCrash(CrashPoint.AFTER_JOURNAL_APPEND, req, shaActual);

            // ---- 阶段3：内存索引更新（提交后立即可见）----
            if (obj == null) {
                obj = new ObjectState();
                space.objects.put(req.objectId(), obj);
            }
            ObjectVersion ov = new ObjectVersion(req.space(), req.objectId(), newVersion, shaActual,
                    req.content().length, req.principal(), retentionUntil, req.legalHold(), now, false);
            obj.versions.put(newVersion, ov);
            space.blobRefs.merge(shaActual, 1, Integer::sum);
            try {
                // 新建块计数从 0 起步、复用块保持当前计数；提交后都 +1
                blobs.incrementRefs(shaActual);
            } catch (IOException e) {
                throw new UncheckedIOException("引用计数更新失败", e);
            }
            space.lastSeq = seq;
            log.info("提交成功并可见: space={}, object={}, version={}, sha256={}, deduplicated={}",
                    req.space(), req.objectId(), newVersion, shaActual, !createdBlob);
            return new PutResult(req.space(), req.objectId(), newVersion, shaActual, !createdBlob);
        }
    }

    // ------------------------------------------------------------ 读取

    public ObjectVersion stat(String space, String objectId, String principal) {
        synchronized (lock) {
            SpaceState st = requireSpace(space);
            AccessControl.checkSpace(st.grants, principal, Permission.READ);
            ObjectState obj = requireObject(st, objectId);
            ObjectVersion head = obj.latest();
            if (head == null || head.deleted()) {
                throw new StoreException(RejectReason.OBJECT_NOT_FOUND,
                        "对象无存活版本: " + space + "/" + objectId);
            }
            return head;
        }
    }

    public ObjectVersion statVersion(String space, String objectId, long version, String principal) {
        synchronized (lock) {
            SpaceState st = requireSpace(space);
            AccessControl.checkSpace(st.grants, principal, Permission.READ);
            ObjectState obj = requireObject(st, objectId);
            ObjectVersion ov = obj.versions.get(version);
            if (ov == null || ov.deleted()) {
                throw new StoreException(RejectReason.VERSION_NOT_FOUND,
                        "版本不存在或已删除: " + space + "/" + objectId + "#v" + version);
            }
            return ov;
        }
    }

    public byte[] get(String space, String objectId, String principal) {
        ObjectVersion v = stat(space, objectId, principal);
        return readBlob(v);
    }

    /** 原子读取最新版本：元数据解析与内容读取在同一把锁内，保证二者属于同一版本。 */
    public VersionContent readLatest(String space, String objectId, String principal) {
        synchronized (lock) {
            SpaceState st = requireSpace(space);
            AccessControl.checkSpace(st.grants, principal, Permission.READ);
            ObjectState obj = requireObject(st, objectId);
            ObjectVersion head = obj.latest();
            if (head == null || head.deleted()) {
                throw new StoreException(RejectReason.OBJECT_NOT_FOUND,
                        "对象无存活版本: " + space + "/" + objectId);
            }
            return new VersionContent(head, readBlob(head));
        }
    }

    /** 原子读取指定版本。 */
    public VersionContent readVersion(String space, String objectId, long version, String principal) {
        synchronized (lock) {
            SpaceState st = requireSpace(space);
            AccessControl.checkSpace(st.grants, principal, Permission.READ);
            ObjectState obj = requireObject(st, objectId);
            ObjectVersion ov = obj.versions.get(version);
            if (ov == null || ov.deleted()) {
                throw new StoreException(RejectReason.VERSION_NOT_FOUND,
                        "版本不存在或已删除: " + space + "/" + objectId + "#v" + version);
            }
            return new VersionContent(ov, readBlob(ov));
        }
    }

    public byte[] getVersion(String space, String objectId, long version, String principal) {
        ObjectVersion v = statVersion(space, objectId, version, principal);
        return readBlob(v);
    }

    private byte[] readBlob(ObjectVersion v) {
        try {
            byte[] data = blobs.get(v.sha256());
            log.info("读取成功: {}/{}#v{}, sha256={}, size={}", v.space(), v.objectId(),
                    v.version(), v.sha256(), data.length);
            return data;
        } catch (IOException e) {
            throw new UncheckedIOException("内容读取失败", e);
        }
    }

    // ------------------------------------------------------------ 删除与回收

    public DeleteResult deleteVersion(String space, String objectId, long version, String principal) {
        log.info("删除请求: space={}, object={}, version={}, principal={}",
                space, objectId, version, principal);
        synchronized (lock) {
            SpaceState st = requireSpace(space);
            AccessControl.checkSpace(st.grants, principal, Permission.DELETE);
            ObjectState obj = requireObject(st, objectId);
            ObjectVersion ov = obj.versions.get(version);
            if (ov == null || ov.deleted()) {
                throw new StoreException(RejectReason.VERSION_NOT_FOUND,
                        "版本不存在或已删除: " + space + "/" + objectId + "#v" + version);
            }
            long now = System.currentTimeMillis();
            if (ov.legalHold()) {
                throw new StoreException(RejectReason.LEGAL_HOLD_ACTIVE,
                        "版本处于合规锁定，禁止删除: " + objectId + "#v" + version);
            }
            if (ov.retentionUntilEpochMillis() > now) {
                throw new StoreException(RejectReason.RETENTION_ACTIVE,
                        "版本处于保留期内，禁止删除: " + objectId + "#v" + version
                                + ", 保留至 " + ov.retentionUntilEpochMillis() + " (now=" + now + ")");
            }

            // 提交删除事件
            long seq = journal.nextSeq();
            journal.append(space, new VersionDeletedEvent(seq, space, objectId, version, principal, now));

            // 更新内存索引
            ObjectVersion deleted = new ObjectVersion(ov.space(), ov.objectId(), ov.version(), ov.sha256(),
                    ov.sizeBytes(), ov.ownerPrincipal(), ov.retentionUntilEpochMillis(),
                    ov.legalHold(), ov.committedAtEpochMillis(), true);
            obj.versions.put(version, deleted);
            int refs = st.blobRefs.merge(ov.sha256(), -1, Integer::sum);
            if (refs <= 0) {
                st.blobRefs.remove(ov.sha256());
            }
            st.lastSeq = seq;

            // 物理回收：仅当任何空间都不再有存活版本引用该内容块时才删除磁盘文件。
            // 全局 .refs 文件按所有空间的存活版本计数；本空间内存计数为权威判定的第一道关。
            boolean reclaimed = false;
            try {
                int globalRefs = blobs.decrementRefs(ov.sha256());
                if (!st.blobRefs.containsKey(ov.sha256()) && !globalBlobInUseElsewhere(ov.sha256(), space)) {
                    blobs.reclaimIfUnreferenced(ov.sha256());
                    reclaimed = true;
                } else {
                    log.info("内容块仍被引用，保留磁盘块: sha256={}, globalRefs={}",
                            ov.sha256(), Math.max(globalRefs, 0));
                }
            } catch (StoreException e) {
                if (e.getReason() != RejectReason.BLOB_IN_USE) {
                    throw e;
                }
                log.info("内容块仍被引用，保留磁盘块: sha256={}", ov.sha256());
            } catch (IOException e) {
                throw new UncheckedIOException("内容回收失败", e);
            }
            log.info("删除成功: {}/{}#v{}, blobReclaimed={}", space, objectId, version, reclaimed);
            return new DeleteResult(space, objectId, version, reclaimed);
        }
    }

    // ------------------------------------------------------------ 崩溃恢复

    public void recover() {
        synchronized (lock) {
            log.info("开始崩溃恢复: root 扫描日志并重放");
            try {
                blobs.cleanStaging();
            } catch (IOException e) {
                throw new UncheckedIOException("清理暂存区失败", e);
            }
            spaces.clear();
            List<String> spaceNames;
            try {
                spaceNames = journal.listSpaces();
            } catch (IOException e) {
                throw new UncheckedIOException("扫描日志失败", e);
            }
            for (String spaceName : spaceNames) {
                List<Event> events;
                try {
                    events = journal.replay(spaceName);
                } catch (IOException e) {
                    throw new UncheckedIOException("日志重放失败: " + spaceName, e);
                }
                SpaceState st = new SpaceState();
                for (Event e : events) {
                    apply(st, spaceName, e);
                }
                spaces.put(spaceName, st);
                log.info("空间[{}]恢复: 对象数={}, 去重内容块数={}, 占用字节={}",
                        spaceName, st.objects.size(), st.blobRefs.size(), usedBytes(st));
            }
            reconcileBlobs();
            log.info("崩溃恢复完成: 空间数={}, 全局最大序号={}", spaces.size(), journal.currentSeq());
        }
    }

    private void apply(SpaceState st, String spaceName, Event e) {
        switch (e) {
            case SpaceCreatedEvent sce -> {
                st.owner = sce.ownerPrincipal();
                st.quotaBytes = sce.quotaBytes();
                st.grants = new HashMap<>(sce.grants());
                st.lastSeq = sce.seq();
            }
            case VersionCommittedEvent vce -> {
                ObjectState obj = st.objects.computeIfAbsent(vce.objectId(), k -> new ObjectState());
                ObjectVersion ov = new ObjectVersion(vce.space(), vce.objectId(), vce.version(),
                        vce.sha256(), vce.sizeBytes(), vce.ownerPrincipal(),
                        vce.retentionUntilEpochMillis(), vce.legalHold(), vce.committedAtEpochMillis(), false);
                obj.versions.put(vce.version(), ov);
                st.blobRefs.merge(vce.sha256(), 1, Integer::sum);
                st.lastSeq = vce.seq();
            }
            case VersionDeletedEvent vde -> {
                ObjectState obj = st.objects.get(vde.objectId());
                if (obj != null) {
                    ObjectVersion ov = obj.versions.get(vde.version());
                    if (ov != null && !ov.deleted()) {
                        obj.versions.put(vde.version(), new ObjectVersion(ov.space(), ov.objectId(),
                                ov.version(), ov.sha256(), ov.sizeBytes(), ov.ownerPrincipal(),
                                ov.retentionUntilEpochMillis(), ov.legalHold(),
                                ov.committedAtEpochMillis(), true));
                        int refs = st.blobRefs.merge(ov.sha256(), -1, Integer::sum);
                        if (refs <= 0) {
                            st.blobRefs.remove(ov.sha256());
                        }
                    }
                }
                st.lastSeq = vde.seq();
            }
        }
    }

    /** 用日志重建出的权威引用计数协调磁盘块：修复计数文件、回收孤儿块、报告缺失块。 */
    private void reconcileBlobs() {
        Map<String, Integer> globalLiveRefs = new HashMap<>();
        for (SpaceState st : spaces.values()) {
            st.blobRefs.forEach((sha, n) -> globalLiveRefs.merge(sha, n, Integer::sum));
        }
        List<String> onDisk;
        try {
            onDisk = blobs.listBlobIds();
        } catch (IOException e) {
            throw new UncheckedIOException("扫描内容块失败", e);
        }
        for (String sha : onDisk) {
            try {
                blobs.reconcile(sha, globalLiveRefs.getOrDefault(sha, 0));
            } catch (IOException e) {
                throw new UncheckedIOException("内容块协调失败: " + sha, e);
            }
        }
        globalLiveRefs.keySet().stream()
                .filter(sha -> !onDisk.contains(sha))
                .forEach(sha -> log.error("已提交版本引用的内容块在磁盘上缺失: sha256={}", sha));
    }

    List<Event> debugEvents(String space) {
        try {
            return journal.replay(space);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 测试/诊断：空间内各去重内容块的存活引用计数。 */
    public Map<String, Integer> debugBlobRefs(String space) {
        synchronized (lock) {
            return Map.copyOf(requireSpace(space).blobRefs);
        }
    }

    private volatile CrashPoint armedCrash;

    /** 测试专用：在当前实例上下一次提交的指定阶段注入进程崩溃（halt）。 */
    public void armCrashForNextCommit(CrashPoint point, String shaFilter) {
        this.armedCrash = point;
        this.crashShaFilter = shaFilter;
    }

    private volatile String crashShaFilter;

    // ------------------------------------------------------------ 内部辅助

    private boolean globalBlobInUseElsewhere(String sha, String currentSpace) {
        for (Map.Entry<String, SpaceState> e : spaces.entrySet()) {
            if (!e.getKey().equals(currentSpace) && e.getValue().blobRefs.containsKey(sha)) {
                return true;
            }
        }
        return false;
    }

    private long usedBytes(SpaceState st) {
        long total = 0;
        for (String sha : st.blobRefs.keySet()) {
            // 大小可从任一存活版本元数据获取（同一 sha 大小必然相同）
            total += sizeOfBlob(st, sha);
        }
        return total;
    }

    private long sizeOfBlob(SpaceState st, String sha) {
        for (ObjectState obj : st.objects.values()) {
            for (ObjectVersion v : obj.versions.values()) {
                if (!v.deleted() && v.sha256().equals(sha)) {
                    return v.sizeBytes();
                }
            }
        }
        return 0L;
    }

    private SpaceState requireSpace(String space) {
        SpaceState st = spaces.get(space);
        if (st == null) {
            throw new StoreException(RejectReason.SPACE_NOT_FOUND, "存储空间不存在: " + space);
        }
        return st;
    }

    private ObjectState requireObject(SpaceState st, String objectId) {
        ObjectState obj = st.objects.get(objectId);
        if (obj == null) {
            throw new StoreException(RejectReason.OBJECT_NOT_FOUND,
                    "对象不存在: " + objectId);
        }
        return obj;
    }

    private StoreException reject(RejectReason reason, String message, PutRequest req) {
        log.warn("写入被拒绝: reason={}, space={}, object={}, principal={}, message={}",
                reason, req.space(), req.objectId(), req.principal(), message);
        return new StoreException(reason, message);
    }

    private void maybeCrash(CrashPoint point, PutRequest req, String sha) {
        if (armedCrash != point) {
            return;
        }
        if (crashShaFilter != null && !crashShaFilter.isEmpty()
                && !crashShaFilter.equalsIgnoreCase(sha)) {
            return;
        }
        log.error("测试崩溃注入触发: point={}, space={}, object={}, sha256={}",
                point, req.space(), req.objectId(), sha);
        Runtime.getRuntime().halt(1);
    }
}
