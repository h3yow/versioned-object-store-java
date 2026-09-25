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
```

key 经百分号编码映射为安全路径段，任意 UTF-8 键均可使用且可逆。

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
| `RETENTION_ACTIVE` | 保留期内删除 |
| `COMPLIANCE_LOCKED` | 合规锁定内删除 |

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
```
