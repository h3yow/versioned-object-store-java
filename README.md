# versioned-object-store-java

仅依赖本地磁盘的版本化对象存储核心（Java 21 / Spring Boot 3.4）。不依赖任何外部服务、
真实凭据或付费资源；除工程已有依赖（Spring Boot 自带的 Jackson）外无新增依赖。

## 对象版本模型

- 对象由 `(bucket, key)` 定位，每次成功提交产生一个**不可变版本**，版本号从 1 开始**单调递增**。
- 每个版本携带可追溯元数据 `VersionInfo`：版本号、内容 SHA-256、大小、提交时间、提交者、
  保留截止时间、合规锁定标志。
- **内容去重**：内容按 SHA-256 寻址存入 `blobs/`。同一内容无论写入多少次、写入多少个
  对象，只在磁盘上占用一份存储；重复内容的写入不消耗配额。
- **默认语义**：`WriteRequest.expectedVersion` 为 `null` 时，写入是确定且稳定的
  **无条件追加**（不检查当前版本，直接签发下一个版本号）；携带时期望值必须等于当前
  HEAD，否则拒绝（乐观并发控制）。

### 磁盘布局

```
root/
  blobs/<sha256>                                  内容寻址块（不可变，去重）
  buckets/<bucket>/objects/<key>/
    versions/v-<n>.json                           不可变版本元数据
    HEAD                                          已签发版本号（提交点）
    retention.json                                保留策略
  batches/<batchId>.json                          批量提交记录（整批的提交点与幂等存根）
```

key 经百分号编码映射为安全路径段，任意 UTF-8 键均可使用且可逆。

## 批量提交

`commitBatch(bucket, BatchCommit, principal)` 把多个对象的新增/覆盖写入作为**一个整体**提交：
要么全部可见，要么全部不可见。

- **整体原子可见**：所有版本元数据落盘后，先原子写入提交记录
  `batches/<batchId>.json`（整批的**提交点**），再在可见性写锁内一次性翻转所有对象的
  `HEAD`。读者持读锁，任何一次读操作只会看到整批之前或整批之后的状态，
  永远不会看到只提交了一部分对象的中间状态。
- **与单对象写入兼容**：批内每个对象仍按单对象规则产生不可变版本，版本号从各自
  HEAD 起单调递增、历史版本照旧可查；批量与单对象写入由对象级锁串行化，可安全混用。
  `WriteRequest` 的语义（`expectedVersion` 乐观并发、校验和、非空内容）在批内逐项生效。
- **容量与去重**：整批只按**净新增**内容计配额——批内相同内容只算一份，与已有内容块
  相同的也不算。净新增超出剩余配额时整批拒绝（`QUOTA_EXCEEDED`），且配额检查与内容块
  写入在同一临界区内完成，拒绝后不会残留已占用的空间。
- **整批拒绝，原因可区分**：任一对象越权（`ACCESS_DENIED`）、`expectedVersion` 与当前
  HEAD 冲突（`VERSION_CONFLICT`）、对象处于合规锁定（`COMPLIANCE_LOCKED`）或保留期内
  （`RETENTION_ACTIVE`）、内容为空（`EMPTY_CONTENT`）、校验和不符
  （`CHECKSUM_MISMATCH`）、批次结构非法（`INVALID_BATCH`：标识为空、条目为空、同一
  key 重复）时，整批不落盘，其他对象也不受影响。所有校验在任何写入之前完成。
- **保留规则说明**：批量提交对目标对象做写入前保留检查——合规锁定或保留期内的对象
  不允许被覆盖产生新版本（单对象 `put` 行为保持不变，不做破坏性变更）。

### 崩溃恢复与幂等重试

- **崩溃一致性**：提交记录落盘前崩溃，整批不算数——重启恢复会清理悬空的版本元数据，
  未引用的内容块由 `garbageCollect` 回收；提交记录落盘后崩溃，整批算成功——重启恢复
  会根据提交记录把所有对象的 HEAD 补齐，整批完整可读。两种情形都不会留下半批数据。
- **幂等重试**：客户端用同一 `batchId` 重试（如网络抖动后）时，已提交的批次直接回放
  提交记录中的稳定结果（`BatchResult.idempotentReplay=true`），不重复产生版本、不重复
  占用空间；重试结果与首次完全一致。同一 `batchId` 被**不同内容**复用时拒绝
  （`BATCH_ID_REUSED`）。

## 并发与恢复语义

- **提交协议**（对象级锁内执行）：校验 → 写新内容块（tmp + fsync + 原子 rename，并计配额）
  → 写版本元数据（tmp + fsync + 原子 rename 为 `v-<n>.json`）→ 原子更新 `HEAD`
  → fsync 目录。`HEAD` 是**提交点**。
- **无半提交可见状态**：读者只认可版本号 ≤ HEAD 的版本文件。元数据先落盘、HEAD 后更新，
  因此并发读者永远不会看到未完成的版本。
- **并发提交**：同一对象的提交由对象级锁串行化，已提交版本单调递增且无间隙；
  乐观并发（`expectedVersion`）下同一轮只有一个提交者成功，其余得到 `VERSION_CONFLICT`。
- **崩溃恢复**：构造 `LocalObjectStore` 时自动执行恢复——删除所有 `*.tmp` 临时文件、
  删除版本号 > HEAD 的悬空版本文件（从未提交，不算成功）。已提交状态在重启后完整可见，
  去重关系保持。未完成的写入留下的未引用内容块由 `garbageCollect` 回收。

## 访问控制与保留删除规则

- 基于 `(bucket, principal)` 的角色授予：`READER < WRITER < ADMIN`（`LocalAccessControl`）。
  读需要 READER，写/删除/设置保留策略需要 WRITER，创建 bucket 者自动成为其 ADMIN。
- **保留期**：`RetentionPolicy.retentionUntil` 之前删除对应版本被拒绝（`RETENTION_ACTIVE`）；
  过期后允许删除。
- **合规锁定**：`complianceLock=true` 的版本任何删除都被拒绝（`COMPLIANCE_LOCKED`），
  优先于保留期判断。
- **回收规则**：删除版本只删除元数据；内容块仅当不再被**任何**对象的**任何**版本引用时，
  才由 `garbageCollect` 回收并释放配额。

### 可区分的拒绝原因（`StoreException.code()`）

| ErrorCode | 含义 |
|---|---|
| `CHECKSUM_MISMATCH` | 声明的 SHA-256 与实际内容不符（读取时校验失败亦同） |
| `EMPTY_CONTENT` | 空内容写入 |
| `QUOTA_EXCEEDED` | 新内容块超出存储配额（去重命中不计） |
| `ACCESS_DENIED` | 越权访问 |
| `OBJECT_NOT_FOUND` | 对象/版本/bucket 不存在 |
| `VERSION_CONFLICT` | `expectedVersion` 与当前 HEAD 不一致 |
| `RETENTION_ACTIVE` | 保留期内删除；批量提交覆盖保留期内对象 |
| `COMPLIANCE_LOCKED` | 合规锁定内删除；批量提交覆盖合规锁定对象 |
| `INVALID_BATCH` | 批量提交结构非法（标识为空、条目为空、key 重复） |
| `BATCH_ID_REUSED` | 同一批量标识被不同内容的提交复用 |

## 本地验证方法

```bash
mvn test
```

测试覆盖（日志中打印关键输入、版本标识与判定依据）：

- `VersionedWriteTest`：去重（3 次相同内容写入仅 1 个块）、版本单调与不可变、
  默认追加语义、版本冲突、校验和不匹配、空写入、配额超限及去重不占配额。
- `ConcurrencyTest`：16 线程并发提交同一对象（版本唯一、无间隙、无半提交）、
  乐观并发下同一轮仅一人成功、并发读写时读者永远看到完整一致的内容。
- `CrashRecoveryTest`：在“元数据已落盘、HEAD 未更新”的崩溃点注入模拟崩溃，
  重启后未提交版本不可见且被清理；已提交状态与去重结果在重启后保持；
  崩溃后重试的提交语义确定。
- `AccessRetentionGcTest`：越权读/写/删除、对象不存在、保留期内删除被拒、
  保留期过期后删除成功、合规锁定删除被拒、内容块仅在无引用时回收。
- `BatchCommitTest`：批量提交的原子性与提交中并发读取（读者只见整批前/后状态）、
  批内去重与已有内容复用、配额超限整批拒绝且无残余占用、容量边界恰好可写、
  权限/版本冲突/保留规则导致整批拒绝且原因可区分、提交点前/后崩溃的重启恢复、
  同一标识重试幂等回放与标识复用拒绝。

最小使用示例：

```java
LocalAccessControl acl = new LocalAccessControl();
LocalObjectStore store = new LocalObjectStore(Path.of("/tmp/store"), 1L << 30, acl);
store.createBucket("docs", "alice");           // alice 成为 docs 的 ADMIN
acl.grant("docs", "bob", Role.WRITER);

VersionInfo v1 = store.put("docs", "a.txt",
        WriteRequest.of("hello".getBytes()), "bob");
byte[] data = store.get("docs", "a.txt", "alice");
store.setRetention("docs", "a.txt",
        RetentionPolicy.until(Instant.now().plus(Duration.ofDays(7))), "bob");

// 批量提交：单据与附属文件作为一个整体提交，可用同一 batchId 安全重试
BatchResult result = store.commitBatch("docs", new BatchCommit("submit-42", List.of(
        BatchItem.of("bill.json", billBytes),
        BatchItem.of("attach-1.pdf", pdfBytes),
        new BatchItem("attach-2.pdf",
                new WriteRequest(pdf2Bytes, 3L, null))  // 乐观并发条件逐项生效
)), "bob");
```
